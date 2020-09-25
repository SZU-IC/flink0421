package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink13_Transform_Select {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //从文件中读取数据
        final DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        final SingleOutputStreamOperator<WaterSensor> sensorDs = fileDS.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                final String[] datas = value.split(",");
                return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
            }
        });


        // TODO Split: 水位低于 50 正常，水位 [50，80) 警告， 水位高于 80 告警
        // split并不是真正的把流分开,只是在上面做标签
        final SplitStream<WaterSensor> splitDS = sensorDs.split(new OutputSelector<WaterSensor>() {
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

        //TODO select
        // 通过之前的标签名，获取对应的流
        // 一个流可以起多个名字，取出的时候，给定一个名字就行
        splitDS.select("normal").print("normal");
        splitDS.select("warn").print("warn");
        splitDS.select("alarm").print("alarm");

        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
