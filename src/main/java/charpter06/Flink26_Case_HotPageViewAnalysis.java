package charpter06;

import bean.ApacheLog;
import bean.HotPageView;
import bean.SimpleAggFunction;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;


/**
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 *
 * @author cjp
 * @version 1.0
 * @date 2020/9/23 16:06
 */
public class Flink26_Case_HotPageViewAnalysis {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //读取数据
        final SingleOutputStreamOperator<ApacheLog> logDS = env.readTextFile("input/apache.log")
                .map(
                        new MapFunction<String, ApacheLog>() {
                            @Override
                            public ApacheLog map(String value) throws Exception {
                                final String[] datas = value.split(" ");
                                //处理数据格式，转成时间戳
                                final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                                final Date date = sdf.parse(datas[3]);
                                //获取时间戳
                                long ts = date.getTime();
                                return new ApacheLog(
                                        datas[0],
                                        datas[1],
                                        ts,
                                        datas[5],
                                        datas[6]
                                );
                            }
                        })
                .assignTimestampsAndWatermarks(
                        //TODO 设置乱序的程度，等待时间
                        new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.minutes(1)) {
                            @Override
                            public long extractTimestamp(ApacheLog element) {
                                return element.getEventTime();
                            }
                        }
                );

        //处理数据
        // 按照统计维度 分组 url
        logDS.keyBy(data -> data.getUrl())
                .timeWindow(Time.minutes(10),Time.seconds(5))
                //聚合之后给窗口划定界线
                .aggregate(
                        new SimpleAggFunction<ApacheLog>(),
                        new CountResultWithWindowEnd()
                )
                //根据上面记录的窗口时间进行分组
                .keyBy(data -> data.getWindowEnd())
                .process(
                       new TopNPageView(3)
                ).print();
        env.execute();

    }

    public static class TopNPageView extends KeyedProcessFunction<Long,HotPageView,String>{
        private Integer threshold;
        private ListState<HotPageView> datas;
        private ValueState<Long> triggerTs;

        public TopNPageView(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getListState(new ListStateDescriptor<HotPageView>("datas", HotPageView.class));
            triggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("triggerTS", Long.class));
        }

        @Override
        public void processElement(HotPageView value, Context ctx, Collector<String> out) throws Exception {
            //来一条数据存一条
            datas.add(value);
            //TODO 模拟窗口触发，注册定时器，如果定时器为空就注册，不然就不注册
            if(triggerTs.value() == null){
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<HotPageView> hotPageViews = new ArrayList<>();
            for (HotPageView hotPageView : datas.get()) {
                hotPageViews.add(hotPageView);
            }
            //清空
            datas.clear();
            triggerTs.clear();

            //排序
            hotPageViews.sort(new Comparator<HotPageView>() {
                @Override
                public int compare(HotPageView o1, HotPageView o2) {
                    return o2.getViewCount().intValue() - o1.getViewCount().intValue();
                }
            });

            //取前N个
            final StringBuilder resultStr = new StringBuilder();
            resultStr.append("窗口结束时间:" + (timestamp - 10) + "\n")
                    .append("---------------------------------------------------\n");

            for (int i = 0; i < threshold; i++) {
                resultStr.append(hotPageViews.get(i) + "\n");
            }

            resultStr.append("--------------------------------------------------\n\n");

            out.collect(resultStr.toString());

        }
    }

    public static class CountResultWithWindowEnd extends ProcessWindowFunction<Long, HotPageView,String,TimeWindow>{
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<HotPageView> out) throws Exception {
            out.collect(new HotPageView(s,elements.iterator().next(),context.window().getEnd()));
        }
    }
}
