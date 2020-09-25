package charpter06;

import bean.WaterSensor;
import com.sun.corba.se.spi.ior.iiop.MaxStreamFormatVersionComponent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import javax.swing.text.AbstractDocument;

/**
 * @author Wither
 * 2020/9/23
 * charpter06
 */
public class Flink14_ProcessFunction_Keyed {
    public static void main(String[] args) {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // TODO 1.env指定时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop112", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })

                //TODO 自定义，定时器的触发需要 <= watermark，在官方的asscending中，watermark = end -1 ，所以定时器的触发要小于end，可以自定义watermark
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            private Long maxTs = Long.MAX_VALUE;
                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                maxTs = Math.max(maxTs, extractedTimestamp);
                                return new Watermark(maxTs);
                            }
                        }
                );

        final SingleOutputStreamOperator<Long> sensorKS = sensorDS
                .keyBy(data -> data.getId())
                .process(
                        new KeyedProcessFunction<String, WaterSensor, Long>() {
                            private Long triggers = 0L;

                            /**
                             * 到了定时的时间，要干什么
                             * @param timestamp 注册的定时器的时间
                             * @param ctx   上下文
                             * @param out   采集器
                             * @throws Exception
                             */
                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Long> out) throws Exception {
                                System.out.println(timestamp + "定时器触发");
                            }


                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<Long> out) throws Exception {
                                if (triggers == null) {
                                    //设置定时器的时间为事件时间 + 5000L
                                    ctx.timerService().registerEventTimeTimer(value.getTs() * 1000L + 5000L);
                                    triggers = value.getTs() * 1000L + 5000L;
                                }

                            }
                        }

                );

        sensorKS.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
