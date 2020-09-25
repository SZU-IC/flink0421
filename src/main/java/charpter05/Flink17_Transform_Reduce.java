package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink17_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
//                .readTextFile("input/sensor-data.log");
                .socketTextStream("hadoop112",9999);

        //将数据转换为实体对象
        final SingleOutputStreamOperator<WaterSensor> seneorDs = inputDS.map(new Flink06_Transform_Map.MyMapFunction());

        //按照 id 进行分组
        final KeyedStream<Tuple3<String, Long, Integer>, String> sensorKS = seneorDs.map(new MapFunction<WaterSensor, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(WaterSensor value) throws Exception {
                return new Tuple3<>(value.getId(), Long.valueOf(value.getTs()), Integer.valueOf(value.getVc()));
            }
        }).keyBy(r -> r.f0);

        // TODO Reduce
        // 1.输入的类型要一致，输出的类型也要一致
        // 2.第一条来的数据，不会进入reduce
        // 3.帮我们保存了中间状态
        sensorKS
                .reduce(new ReduceFunction<Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> reduce(Tuple3<String, Long, Integer> value1, Tuple3<String, Long, Integer> value2) throws Exception {
                        System.out.println(value1.toString() + "<->" + value2.toString());
                        return Tuple3.of("aaa", 123L, value1.f2 + value2.f2);
                    }
                }).print("reduce");
        env.execute();
    }


    /*public static class MyKeySelector implements KeySelector<WaterSensor, String> {

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }

    }*/
}
