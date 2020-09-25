package charpter05;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink01_Environment {
    public static void main(String[] args) {
         //获取 批处理 执行环境
        final ExecutionEnvironment benv = ExecutionEnvironment.getExecutionEnvironment();
        //获取 流处理 执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
