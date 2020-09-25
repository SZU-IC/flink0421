package charpter06;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.lang.model.element.VariableElement;

/**
 * @author Wither
 * 2020/9/22
 * charpter06
 */
public class Flink11_Watermark_AllowedLateness {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //
        final SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop112", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                .assignTimestampsAndWatermarks(
                        //TODO 乱序使用这个接口
                        new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(WaterSensor element) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        //分组、开窗、聚合
        //TODO 处理迟到的数据： 窗口再等一会


        // TODO 2.当watermark >= 窗口结束时间 + 窗口等待时间，会真正的关闭窗口。
        // TODO 2.当 窗口结束时间 <= watermark <= 窗口结束时间 + 窗口等待时间,每来一条迟到数据，就会计算一次。
        sensorDS
                .keyBy(data -> data.getId())
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
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
