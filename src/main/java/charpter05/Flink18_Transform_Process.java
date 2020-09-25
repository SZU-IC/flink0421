package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink18_Transform_Process {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        final DataStreamSource<String> inputDS = senv.readTextFile("input/sensor-data.log");

        // 2.Transform: Map转换成实体对象
        final SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new Flink06_Transform_Map.MyMapFunction());

        //按照 id 进行分组
        final KeyedStream<Tuple3<String, Long, Integer>, String> sensorKS = sensorDS.map(new MapFunction<WaterSensor, Tuple3<String, Long, Integer>>() {
            @Override
            public Tuple3<String, Long, Integer> map(WaterSensor value) throws Exception {
                return new Tuple3<String, Long, Integer>(value.getId(), Long.valueOf(value.getTs()), Integer.valueOf(value.getVc()));
            }
        }).keyBy(r -> r.f0);
        
        //TODO Process
        //可以获取溢写环境
        sensorKS.process(new KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>() {
            /**
              * @Author:Wither
              * @Description:来一条数据处理一条
             *  ctx:上下文
             *  out：采集器
             *  value:一条数据
              * @Date:20:08 2020/9/18
              */
            @Override
            public void processElement(Tuple3<String, Long, Integer> value, Context ctx, Collector<String> out) throws Exception {
                out.collect("当前key=" + ctx.getCurrentKey() + "当前时间="+ ctx.timestamp() + "当前数据="+ value);
            }
        }).print("process");

        senv.execute();
    }
}
