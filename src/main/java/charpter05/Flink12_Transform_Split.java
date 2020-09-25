package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink12_Transform_Split {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        //从文件读取数据
        final SingleOutputStreamOperator<WaterSensor> sensorDS = senv.readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        //TODO Split: 水位低于 50 正常，水位 [50,80) 警告， 水位高于 80 警告
        final SplitStream<WaterSensor> resultDS = sensorDS.split(new OutputSelector<WaterSensor>() {
            @Override
            public Iterable<String> select(WaterSensor value) {
                if (value.getVc() < 50) {
                    return Arrays.asList("normal");
                } else if (value.getVc() < 80) {
                    return Arrays.asList("warn");
                } else {
                    return Arrays.asList("alarm");
                }
            }
        });

        resultDS.print();
        senv.execute();

    }
}
