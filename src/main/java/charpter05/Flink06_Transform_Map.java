package charpter05;

import akka.remote.artery.aeron.AeronSink;
import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink06_Transform_Map {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(2);
        //从文件读取数据
        final DataStreamSource<String> inputDS = senv.readTextFile("input/sensor-data.log");
        //Transform：Map转换成实体对象
        final SingleOutputStreamOperator<WaterSensor> mapDS = inputDS.map(new MyMapFunction());
        //打印
        mapDS.print();
        //启动
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class MyMapFunction implements MapFunction<String, WaterSensor>{

        @Override
        public WaterSensor map(String value) throws Exception {
            final String[] datas = value.split(",");
            return new WaterSensor(datas[0],Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
        }
    }
}
