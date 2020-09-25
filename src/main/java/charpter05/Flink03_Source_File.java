package charpter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink03_Source_File {
    public static void main(String[] args) {
        //获取执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度：全局设置
        senv.setParallelism(1);
        //获取数据
        final DataStreamSource<String> fileDs = senv.readTextFile("input/WC.txt");
        //打印
        fileDs.print();
        //启动
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
