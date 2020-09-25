package charpter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author Wither
 * 2020/9/22
 * charpter06
 */
public class Flink02_2_Window_IncreAgg {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final KeyedStream<Tuple2<String, Integer>, String> dataKS = env.socketTextStream("hadoop112", 9999)
                .map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String value) throws Exception {
                                return Tuple2.of(value, 1);
                            }
                        }
                )
                .keyBy(r -> r.f0);

        final WindowedStream<Tuple2<String, Integer>, String, TimeWindow> dataWS = dataKS.timeWindow(Time.seconds(5));

        dataWS
                /*.reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                            }
                        }
                )*/
                .aggregate(
                        new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                                System.out.println("add...");
                                return 1 + accumulator;
                            }

                            @Override
                            public Integer getResult(Integer accumulator) {
                                System.out.println("get result...");
                                return accumulator;
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        }
                )






                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
