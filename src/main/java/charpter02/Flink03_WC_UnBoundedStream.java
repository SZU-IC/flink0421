package charpter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/15
 * PACKAGE_NAME
 */
public class Flink03_WC_UnBoundedStream {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        final DataStreamSource<String> socketDS = env.socketTextStream("hadoop112", 9999);
        //处理数据
        final SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneTuple = socketDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
            final String[] words = value.split(" ");
            for (String word : words) {
                final Tuple2<String, Integer> tuple = new Tuple2<>(word, 1);
                out.collect(tuple);
            }
        }).returns(new TypeHint<Tuple2<String,Integer>>(){});

        //按照word进行分组
        final KeyedStream<Tuple2<String, Integer>, Tuple> wordAndOneKS = wordAndOneTuple.keyBy(0);
        //聚合
        final SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneKS.sum(1);

        //输出
        resultDS.print();
        //4.启动
        env.execute();
    }
}
