package charpter06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author Wither
 * 2020/9/21
 * charpter06
 */
public class Flink06_Watermark_Ascending {
    public static void main(String[] args) {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.指定语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.
        final SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop112", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                // TODO 2.指定如何 从数据中 抽取出 事件时间，时间单位是 ms
                // Ascending是官方提供的一个抽象类，适用于 数据的 事件时间 是 单调递增 的情况
                // new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
                // 升序的场景下，watermark = eventtime - 1ms
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<WaterSensor>() {
                            @Override
                            public long extractAscendingTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        });

        //分组、开窗、聚合
        sensorDS
                .keyBy(data -> data.getId())
                .timeWindow(Time.seconds(5))
                .process(
                        new ProcessWindowFunction<WaterSensor, Long, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<Long> out) throws Exception {
                                out.collect(elements.spliterator().estimateSize());
                            }
                        }
                ).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
