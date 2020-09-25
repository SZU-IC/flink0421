package charpter05;

import bean.WaterSensor;
import javafx.scene.chart.ValueAxis;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Arrays;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink14_Transform_Connect {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //从文件读取数据
        final SingleOutputStreamOperator<WaterSensor> mapDs = senv.readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                });

        //再获取一条流
        final DataStreamSource<Integer> numDS = senv.fromCollection(Arrays.asList(1, 2, 3, 4));

        // TODO 使用connect连接两条流
        // 两条流 数据类型 可以不一样
        // 只能两条流进行连接
        // 处理数据的时候，也是分开处理
        final ConnectedStreams<WaterSensor, Integer> conDS = mapDs.connect(numDS);

        //TODO 并没有真正连接起来，输出的时候也是分开处理
        //ConnectedStreams没有 print()方法
        final SingleOutputStreamOperator<Object> resultDs = conDS.map(new CoMapFunction<WaterSensor, Integer, Object>() {

            @Override
            public Object map1(WaterSensor value) throws Exception {
                return value.toString();
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value + 10;
            }
        });
        resultDs.print();

        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
