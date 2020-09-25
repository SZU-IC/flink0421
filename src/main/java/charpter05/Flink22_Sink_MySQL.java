package charpter05;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink22_Sink_MySQL {
    public static void main(String[] args) {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.从文件读取数据
        DataStreamSource<String> inputDS = env
                .readTextFile("input/sensor-data.log");
//                .socketTextStream("localhost", 9999);

        //TODO 数据 Sink到自定义 MySQL
        inputDS.addSink(
                new RichSinkFunction<String>() {
                    private Connection conn = null;
                    private PreparedStatement pstmt = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = DriverManager.getConnection("jdbc:mysql://hadoop112:3306/test","root","000000");
                        pstmt = conn.prepareStatement("INSERT INTO sensor VALUES (?,?,?)");
                    }

                    @Override
                    public void close() throws Exception {
                        //与创建的方向相反
                        pstmt.close();
                        conn.close();
                    }

                    @Override
                    public void invoke(String value, Context context) throws Exception {
                        String[] datas = value.split(",");
                        pstmt.setString(1, datas[0]);
                        pstmt.setLong(2, Long.valueOf(datas[1]));
                        pstmt.setInt(3, Integer.valueOf(datas[2]));
                        pstmt.execute();
                    }
                }
        );
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
