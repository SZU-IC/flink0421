package charpter05;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink02_Source_Collection {
    public static void main(String[] args)  {
        //获取执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        final DataStreamSource<WaterSensor> sensorDS = senv.fromCollection(
                Arrays.asList(
                        new WaterSensor("sensor_01", 12348764328L, 41),
                        new WaterSensor("sensor_02", 15837563457L, 43),
                        new WaterSensor("sensor_03", 12425236367L, 49)
                )
        );
        //处理数据
        sensorDS.print();
        //执行
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
