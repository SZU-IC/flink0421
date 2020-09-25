package charpter06;

import bean.AdClickLog;
import bean.HotAdClickByUser;
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
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
  * @Author:Wither
  * @Description:每隔5秒，输出最近10分钟内不同用户点击最多的广告排名
  * @Date:10:56 2020/9/24
  */
public class Flink28_Case_AdClickAnalysisByUser {
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
                //指定watermark的时间戳
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<AdClickLog>() {
                            @Override
                            public long extractAscendingTimestamp(AdClickLog element) {
                                return element.getTimestamp() * 1000L;
                            }
                        }
                );


        //2.处理数据
        // 2.1 按照统计维度分组 : 省份、广告
        final KeyedStream<AdClickLog, Tuple2<Long, Long>> adClickDS = logDS.keyBy(
                new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> getKey(AdClickLog value) throws Exception {
                        return Tuple2.of(value.getUserId(), value.getAdId());
                    }
                }
        );

        //2.2 开窗
        adClickDS
                //.timeWindow(Time.minutes(10),Time.seconds(5))
                .timeWindow(Time.hours(1),Time.minutes(5))
                .aggregate(
                        new SimpleAggFunction<AdClickLog>(),
                        new AdCountResultWithWindowEnd()
                )
                .keyBy(data -> data.getWindowEnd())
                .process(
                    new TopNAdClick(3)
                ).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TopNAdClick extends KeyedProcessFunction<Long, HotAdClickByUser, String>{

        private Integer threshold;
        private ListState<HotAdClickByUser> datas;
        private ValueState<Long> triggersTs;

        public TopNAdClick(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<HotAdClickByUser>("datas", HotAdClickByUser.class));
            triggersTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggersTs", Long.class));
        }

        @Override
        public void processElement(HotAdClickByUser value, Context ctx, Collector<String> out) throws Exception {
            datas.add(value);
            if(triggersTs.value() ==  null){
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                triggersTs.update(value.getWindowEnd() + 10);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            final ArrayList<HotAdClickByUser> hotAdClickByUsers = new ArrayList<>();
            for (HotAdClickByUser hotAdClickByUser : datas.get()) {
                hotAdClickByUsers.add(hotAdClickByUser);
            }
            datas.clear();
            triggersTs.clear();
            // 排序
            hotAdClickByUsers.sort(
                    new Comparator<HotAdClickByUser>() {
                        @Override
                        public int compare(HotAdClickByUser o1, HotAdClickByUser o2) {
                            return o2.getClickCount().intValue()-o1.getClickCount().intValue();
                        }
                    }
            );
            // 取前 N 个
            StringBuilder resultStr = new StringBuilder();
            resultStr.append("窗口结束时间:" + (timestamp - 10) + "\n")
                    .append("---------------------------------------------------\n");

            // 加一个判断逻辑： threshold 是否超过 list的大小
            threshold = threshold > hotAdClickByUsers.size() ? hotAdClickByUsers.size() : threshold;
            for (int i = 0; i < threshold; i++) {
                resultStr.append(hotAdClickByUsers.get(i) + "\n");
            }
            resultStr.append("--------------------------------------------------\n\n");

            out.collect(resultStr.toString());
        }
    }
    public static class AdCountResultWithWindowEnd extends ProcessWindowFunction<Long, HotAdClickByUser,Tuple2<Long, Long>, TimeWindow>{
        @Override
        public void process(Tuple2<Long, Long> key, Context context, Iterable<Long> elements, Collector<HotAdClickByUser> out) throws Exception {
            out.collect(new HotAdClickByUser(key.f0,key.f1,elements.iterator().next(),context.window().getEnd()));
        }
    }
}
