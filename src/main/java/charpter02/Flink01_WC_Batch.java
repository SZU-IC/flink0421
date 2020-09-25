package charpter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Wither
 * 2020/9/15
 * PACKAGE_NAME
 */
public class Flink01_WC_Batch {
    public static void main(String[] args) throws Exception {
        //获取上下文环境
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        final DataSource<String> fileDS = env.readTextFile("input/WC.txt");
        //处理数据
        final FlatMapOperator<String, Tuple2<String, Integer>> result = fileDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            final String[] words = value.split(" ");
            for (String word : words) {
                final Tuple2<String, Integer> tuple = new Tuple2<>(word, 1);
                out.collect(tuple);
            }
        }).returns(new TypeHint<Tuple2<String, Integer>>() {
        });
        final UnsortedGrouping<Tuple2<String, Integer>> result1 = result.groupBy(0);
        final AggregateOperator<Tuple2<String, Integer>> sum = result1.sum(1);
        //输出数据
        sum.print();
    }


    public static class MyFlatMapFunction implements FlatMapFunction<String,Tuple2<String,Integer>>{


        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            final String[] words = value.split(" ");

            for (String word : words) {
                final Tuple2<String, Integer> tuple = new Tuple2<>(word, 1);
                out.collect(tuple);
            }
        }
    }
}

