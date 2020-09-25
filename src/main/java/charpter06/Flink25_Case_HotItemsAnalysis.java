package charpter06;

import bean.HotItemCountWithWindowEnd;
import bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.mutable.StringBuilder$;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Wither
 * 2020/9/23
 * charpter06
 */
public class Flink25_Case_HotItemsAnalysis {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        final SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Long.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                }).assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<UserBehavior>() {
                            @Override
                            public long extractAscendingTimestamp(UserBehavior element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );

        // 2.处理数据
        // 2.1 过滤出 pv 行为
        final SingleOutputStreamOperator<UserBehavior> userBehaviorDSFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        //按照 统计维度 分组: 商品
        final KeyedStream<UserBehavior, Long> userBehaviorKS = userBehaviorDSFilter.keyBy(data -> data.getItemId());

        //开窗:没5分钟输出最近一小时 => 滑动窗口，长度为1小时，步长为5分钟
        final WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWS = userBehaviorKS.timeWindow(Time.hours(1), Time.minutes(5));

        // TODO 2.4 求和统计 => 每个商品被点击多少次 => 调用聚合函数后就没有窗口的概念
        // sum? =》需要转成元组（xxx,1），按照 1 求和 =》求完和怎么排序？(无法排序，求和结果都放到同一个流中，无法隔离窗口)
        // reduce？ => 可以得到统计结果,输入和输出的类型要一致 => 排序？(无法排序，求和结果都放到同一个流中，无法隔离窗口)
        // aggregate => 可以得到统计结果 => 排序？(无法排序，求和结果都放到同一个流中，无法隔离窗口)
        // process => 可以得到统计结果 => 是全窗口函数，会存数据，有oom风险.优先考虑增量居合函数

        // TODO => 调用聚合操作之后，每个窗口的输出都混到一起，没有窗口的概念了
        // => 需求是每小时的 topN，也就是说，排序是本窗口的数据进行排序，不同窗口混到一起怎么办？

        // agggate用法介绍：传两个参数
        //      第一个参数：预聚合函数 => 增量聚合
        //      第二个参数：全窗口函数
        //      预聚合的结果，会作为 全窗口函数的 输入 => 数据量变小了
        //      1001
        //      1001
        //      1001
        //      1001
        //      1002
        //      1002
        //      TODO 传递给全窗口的就是 4,2 两条数据，但是 分组的标签(keyBy之后会额外打个标签) 是能获取到的
        //      => 相当于，每个商品一条统计结果，也就是有几种商品，就有几条数据
        //      => TODO 按照咱们的规模，最多也就几十万上下，没压力
        //      第二个参数：全窗口函数，目的是给统计结果，打上窗口的标签，因为聚合之后窗口就没了
        final SingleOutputStreamOperator<HotItemCountWithWindowEnd> aggDS = userBehaviorWS.aggregate(new AggCount(), new CountResultWithWindowEnd());

        // TODO 按照窗口的结束时间 分组 => 让属于同一窗口的统计结果到一起进行排序
        final SingleOutputStreamOperator<String> topN = aggDS.keyBy(data -> data.getWindowEnd())
                .process(

                        new TopNItems(3));

        topN.print();

        env.execute();


    }

    public static class TopNItems extends KeyedProcessFunction<Long,HotItemCountWithWindowEnd,String>{

        //设置一个listState进行保存来的数据
        private ListState<HotItemCountWithWindowEnd> dataList;

        private Integer threshold;
        //TODO 需要记录定时器的时间
        private ValueState<Long> triggerTs;

        public TopNItems(Integer threshold) {
            this.threshold = threshold;
        }

        /**
          * @Author:Wither
          * @Description:保存当前的状态
          * @Date:18:30 2020/9/23
          */
        @Override
        public void open(Configuration parameters) throws Exception {
            dataList = getRuntimeContext().getListState(new ListStateDescriptor<HotItemCountWithWindowEnd>("default", HotItemCountWithWindowEnd.class));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTs", Long.class));
        }

        /*
            //TODO 来一条处理一条
         */
        @Override
        public void processElement(HotItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            // 排序 需要等到属于 同一个窗口 的数据 都到齐 => 来一条处理一条
            dataList.add(value);
            // 存到什么时候为止？ => 什么时候是到齐？ => 什么时候排序？
            // => 模拟窗口的触发， 注册一个 窗口结束时间 的定时器
            if(triggerTs.value() == null){
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
                triggerTs.update(value.getWindowEnd() + 100L);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            final Iterable<HotItemCountWithWindowEnd> datas = dataList.get();
            //创建一个list把数据复制过来
            List<HotItemCountWithWindowEnd> list = new ArrayList<>();
            for (HotItemCountWithWindowEnd hotItemCountWithWindowEnd : datas) {
                list.add(hotItemCountWithWindowEnd);
            }
            dataList.clear();
            triggerTs.clear();

            //排序方式
            list.sort(new Comparator<HotItemCountWithWindowEnd>() {
                @Override
                public int compare(HotItemCountWithWindowEnd o1, HotItemCountWithWindowEnd o2) {
                    //需要降序
                    return (o2.getItemCount().intValue() - o1.getItemCount().intValue());
                }
            });

            final StringBuilder resultStr = new StringBuilder();
            resultStr
                    .append("窗口结束时间: " + timestamp + "\n")
                    .append("-----------------------------------------------------------------------------\n");

            for (int i = 0; i < threshold; i++) {
                resultStr.append(list.get(i) + "\n");
            }

            resultStr.append("---------------------------------------------------------------------------\n\n");

            out.collect(resultStr.toString());
        }
    }

    /**
     * 全窗口函数：目的是 给 统计结果 打上 窗口结束时间的标签，用于区分 统计结果 属于哪个窗口
     */
    public static class CountResultWithWindowEnd extends ProcessWindowFunction<Long, HotItemCountWithWindowEnd,Long,TimeWindow>{
        @Override
        public void process(Long key, Context context, Iterable<Long> elements, Collector<HotItemCountWithWindowEnd> out) throws Exception {
            //只有一个值所以只需 elements.iterator().next()
            out.collect(new HotItemCountWithWindowEnd(key,elements.iterator().next(),context.window().getEnd()));
        }
    }

    /**
     * 预聚合函数：输出 会传递给 全窗口函数
     */
    public static class AggCount implements AggregateFunction<UserBehavior,Long,Long>{


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        // TODO 回话窗口才会使用
        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }
}
