package charpter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink04_Source_kafka {
    public static void main(String[] args) {
        //获取执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        //获取数据
            //从kafka读取数据
            final Properties prop = new Properties();
            prop.setProperty("bootstrap.servers", "hadoop112:9092");
            prop.setProperty("group.id", "consumer-group");
            prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.setProperty("auto.offset.reset", "latest");

            final DataStreamSource<String> kafkaDS
                    //找接口的实现类Ctrl + Alt + B
                    //尽量使用011
                    = senv.addSource(new FlinkKafkaConsumer011<String>("sensor0421", new SimpleStringSchema(), prop));
            kafkaDS.print();
            //启动
            try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
