package charpter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author Wither
 * 2020/9/17
 * charpter05
 */
public class Flink09_Transform_Filter {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        //获取数据
        final DataStreamSource<Integer> inputDS = senv.fromCollection(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8)
        );
        //处理数据
        final SingleOutputStreamOperator<Integer> filterDS = inputDS.filter(new MyFilterFunction());
        //打印
        filterDS.print();

        senv.execute();
    }
    public static class MyFilterFunction implements FilterFunction<Integer>{
        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }
    }
}
