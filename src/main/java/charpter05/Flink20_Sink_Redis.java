package charpter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import sun.management.Sensor;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink20_Sink_Redis {
    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);

        //TODO 数据 Sink 到 Redis
        //Builder 是 FlinkJedisPoolConfig 的内部类
        final FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop112")
                .setPort(6379)
                .build();

        inputDS.addSink(
                new RedisSink<String>(
                        jedisConfig,
                        new RedisMapper<String>() {
                            //redis 的命令:key 是最外层的 key
                            @Override
                            public RedisCommandDescription getCommandDescription() {
                                return new RedisCommandDescription(RedisCommand.HSET,"sensor0421");
                            }
                            //Hash类型:这个指定的是 hash 的key
                            @Override
                            public String getKeyFromData(String s) {
                                final String[] datas = s.split(",");
                                return datas[1];
                            }

                            // Hash类型: 这个指的是 hash 的value
                            @Override
                            public String getValueFromData(String s) {
                                final String[] datas = s.split(",");
                                return datas[2];
                            }
                        }
                )
        );

        env.execute();
    }
}
