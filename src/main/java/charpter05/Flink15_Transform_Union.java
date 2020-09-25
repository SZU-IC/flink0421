package charpter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink15_Transform_Union {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //获取流
        final DataStreamSource<Integer> numDs = senv.fromCollection(Arrays.asList(1, 2, 3, 4));
        final DataStreamSource<Integer> numDS1 = senv.fromCollection(Arrays.asList(5, 6, 7, 8));
        final DataStreamSource<Integer> numDS2 = senv.fromCollection(Arrays.asList(9, 10, 11, 12));

        //TODO UNION连接流 (要求流的数据类型要相同，可以连接多条流)
        final DataStream<Integer> unionDS = numDs.union(numDS1).union(numDS2);
        unionDS.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 10;
            }
        }).print("union");
        senv.execute();
    }
}
