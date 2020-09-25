package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
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
public class Flink16_Transform_RollingAgg {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //从文件读取数据
        final DataStreamSource<String> inputDS = senv.socketTextStream("hadoop112", 9999);

        //2. Transform: Map 转换成实体对象
        final SingleOutputStreamOperator<WaterSensor> sensorDS
                = inputDS.map(new Flink06_Transform_Map.MyMapFunction());

        //3. 按照 id 分组
        final KeyedStream<Tuple3<String, Long, Integer>, String> sensorKS = sensorDS.map(new MapFunction<WaterSensor, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(WaterSensor value) throws Exception {
                return new Tuple3<>(value.getId(), value.getTs(), value.getVc());
            }
        }).keyBy(r -> r.f0);

        sensorKS.max(2).print("max");

        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
