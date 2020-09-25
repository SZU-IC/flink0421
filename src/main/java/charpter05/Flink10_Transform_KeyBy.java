package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/17
 * charpter05
 */
public class Flink10_Transform_KeyBy {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(2);
        //获取数据
        final DataStreamSource<String> sensorDs = senv.readTextFile("input/sensor-data.log");
        //转换成实体对象
        final SingleOutputStreamOperator<WaterSensor> inputDs = sensorDs.map(new Flink06_Transform_Map.MyMapFunction());
        final KeyedStream<WaterSensor, String> outDs = inputDs.keyBy(new MyKeySelector());
        outDs.print();
        //启动
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyKeySelector implements KeySelector<WaterSensor,String>{

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
