package charpter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Wither
 * 2020/9/15
 * PACKAGE_NAME
 */
public class Flink02_WC_BoundedStream {
    public static void main(String[] args) throws Exception {
        //0.创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.读取数据
        final DataStreamSource<String> fileDS = env.readTextFile("input/WC.txt");
        //2.处理数据
        //2.1 扁平化操作：切分、转换成二元数组
        final SingleOutputStreamOperator wordAndOneTuple = fileDS.flatMap(new MyFlatMapFunction());
        //按照word进行分组，使用keyBy
        final KeyedStream wordAndOneKS = wordAndOneTuple.keyBy(0);
        //按照分组进行聚合
        final SingleOutputStreamOperator result = wordAndOneKS.sum(1);
        //3.输出，保存
        result.print();
        //4.启动执行
        env.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{
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
