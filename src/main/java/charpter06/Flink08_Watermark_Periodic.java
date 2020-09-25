package charpter06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import javax.xml.crypto.Data;

/**
 * @author Wither
 * 2020/9/21
 * charpter06
 */
public class Flink08_Watermark_Periodic {
    public static void main(String[] args) {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1.env指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //TODO 设置 周期性 生成 Watermark的时间间隔，默认200ms，一般不改动,可以通过下面的命令去改动
//        env.getConfig().setAutoWatermarkInterval(5000L);

        // 1.
        final SingleOutputStreamOperator<WaterSensor> sensorDS = env.socketTextStream("hadoop112", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPeriodicWatermarks<WaterSensor>() {
                            private Long maxTs = Long.MIN_VALUE;

                            @Override
                            public Watermark getCurrentWatermark() {
                                System.out.println("Periodic...");
                                return new Watermark(maxTs);
                            }

                            // TODO 当当前的时间戳大于先前的时间戳，水位才会发生改变，而且水位是单调递增的；
                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                maxTs = Math.max(maxTs, element.getTs() * 1000L);
                                return element.getTs() * 1000L;
                            }
                        }
                );

        //分组、开窗、聚合
        final DataStreamSink<Long> sensorKS = sensorDS
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
