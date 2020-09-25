package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink07_Transform_RichMapFunction {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(2);
        //从文件读取数据
        final DataStreamSource<String> fileDS = senv.readTextFile("input/sensor-data.log");
        //处理数据
        final SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(new MyRichMapFunction());
        //打印
        sensorDS.print();
        //启动
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyRichMapFunction extends RichMapFunction<String, WaterSensor> {

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] datas = value.split(",");
            //可以获取上下文环境
            return new WaterSensor(getRuntimeContext().getTaskName() + datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
        }


        //开的次数和并行度一样
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        //关的次数是分区数的2倍
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
    }
}
