package charpter06;

import bean.AdClickLog;
import bean.HotAdClick;
import bean.SimpleAggFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author Wither
 * 2020/9/23
 * charpter06
 */
public class Flink27_Case_AdClickAnalysis {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据、转换
        final SingleOutputStreamOperator<AdClickLog> logDS = env.readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdClickLog>() {
                    @Override
                    public AdClickLog map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new AdClickLog(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                datas[2],
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<AdClickLog>() {
                            //提取时间戳  Extracts the timestamp from the given element.
                            // The timestamp must be monotonically increasing. 必须是单调递增
                            @Override
                            public long extractAscendingTimestamp(AdClickLog element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );
        //处理数据
        //TODO  2.处理数据
        // TODO 2.1 按照 统计维度 分组:省份、广告
        final KeyedStream<AdClickLog, Tuple2<String, Long>> adClickKS = logDS
                .keyBy(
                        //TODO 匿名内部类需要()
                        new KeySelector<AdClickLog, Tuple2<String, Long>>() {

                            @Override
                            public Tuple2<String, Long> getKey(AdClickLog value) throws Exception {
                                return Tuple2.of(value.getProvince(), value.getAdId());
                            }
                        }
                );

        // 2.2 开窗
        adClickKS
                .timeWindow(Time.minutes(10),Time.seconds(5))
                .aggregate(
                        //TODO 增量聚合，只能在一个窗口中，因此在下面有限定
                        new SimpleAggFunction<>(),
                        //TODO 可以指定窗口结束时间
                        new AdCountResultWithWindowEnd()
                )
                //根据窗口进行分组，然后排序
                .keyBy(data -> data.getWindowEnd())
                .process(new TopNAdClick(3)).print();


        //执行
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //根据窗口进行分组排序，上面传下来的是统计的结果是Long类型
    public static class TopNAdClick extends KeyedProcessFunction<Long, HotAdClick, String>{

        private Integer threshold;
        private ListState<HotAdClick> datas;
        //存的是注册的定时器时间
        private ValueState<Long> triggersTs;

        public TopNAdClick(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<HotAdClick>("datas", HotAdClick.class));
            triggersTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggersTs", Long.class));
        }

        @Override
        public void processElement(HotAdClick value, Context ctx, Collector<String> out) throws Exception {
            //来一条数据存一条
            datas.add(value);
            //判断定时器是否有注册了，没有注册就注册定时器
            if(triggersTs.value() == null){
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                //更新
                triggersTs.update(value.getWindowEnd() + 10L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<HotAdClick> hotAdClicks = new ArrayList<>();
            for (HotAdClick hotAdClick : datas.get()) {
                hotAdClicks.add(hotAdClick);
            }
            //将定时器和保存的状态清空
            datas.clear();
            triggersTs.clear();
            //排序
            hotAdClicks.sort(new Comparator<HotAdClick>() {
                @Override
                public int compare(HotAdClick o1, HotAdClick o2) {
                    return o2.getClickCount().intValue() - o1.getClickCount().intValue();
                }
            });
            // 取前 N 个
            StringBuilder resultStr = new StringBuilder();
            resultStr.append("窗口结束时间:" + (timestamp - 10) + "\n")
                    .append("---------------------------------------------------\n");

            // 加一个判断逻辑： threshold 是否超过 list的大小
            threshold = threshold > hotAdClicks.size() ? hotAdClicks.size() : threshold;
            for (int i = 0; i < threshold; i++) {
                resultStr.append(hotAdClicks.get(i) + "\n");
            }
            resultStr.append("--------------------------------------------------\n\n");

            out.collect(resultStr.toString());
        }
    }

    public static class AdCountResultWithWindowEnd extends ProcessWindowFunction<Long, HotAdClick,Tuple2<String,Long>, TimeWindow>{

        @Override
        public void process(Tuple2<String, Long> key, Context context, Iterable<Long> elements, Collector<HotAdClick> out) throws Exception {
            out.collect(new HotAdClick(key.f0,key.f1,elements.iterator().next(),context.window().getEnd()));
        }
    }
}
