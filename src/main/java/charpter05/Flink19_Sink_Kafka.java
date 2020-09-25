package charpter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink19_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        final DataStreamSource<String> inputDS = senv.readTextFile("input/sensor-data.log");
        //TODO 数据 Sink 到 Kafka
        //DataStream调用 addSink =》 注意：不是env来调用
        inputDS.addSink(new FlinkKafkaProducer011<String>
                ("hadoop112:9092", "sensor0421",new SimpleStringSchema()));

        senv.execute();
    }
}
