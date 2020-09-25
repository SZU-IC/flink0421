package charpter05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/17
 * charpter05
 */
public class Flink11_Transform_Shuffle {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(2);

        //从文件读取数据
        final DataStreamSource<String> inputDs = senv.readTextFile("input/sensor-data.log");
        inputDs.print("input");
        //随机分，底层调用了ShufflePartitioner ，里面是random.nextInt()
        final DataStream<String> resultDs = inputDs.shuffle();
        resultDs.print("shuffle");

        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
