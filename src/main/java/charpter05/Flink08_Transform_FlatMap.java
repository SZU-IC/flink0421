package charpter05;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.omg.CORBA.INTERNAL;

import java.util.Arrays;
import java.util.List;

/**
 * @author Wither
 * 2020/9/17
 * charpter05
 */
public class Flink08_Transform_FlatMap {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        //读取数据
        final DataStreamSource<List<Integer>> inputDS = senv.fromCollection(
                Arrays.asList(
                        Arrays.asList(1, 2, 3, 4),
                        Arrays.asList(5, 6, 7, 8)
                )
        );

        inputDS.flatMap(new MyFlatMapFunction()).print();

        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    public static class MyFlatMapFunction implements FlatMapFunction<List<Integer>, Integer>{

        @Override
        public void flatMap(List<Integer> value, Collector<Integer> out) throws Exception {
            for (Integer number : value) {
                out.collect(number);
            }
        }
    }
}
