package charpter05;

import bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author Wither
 * 2020/9/16
 * charpter05
 */
public class Flink05_Source_MySource {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO Source:从自定义数据源读取
        final DataStreamSource inputDs = senv.addSource(new MySourceFunction());
        //打印
        inputDs.print();
        //启动
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //要指定WaterSensor的类型，不然会报错
public static class MySourceFunction implements SourceFunction<WaterSensor>{
    private boolean flag = true;
    @Override
    public void run(SourceContext ctx) throws Exception {
        final Random random = new Random();

        while (flag){
            //往下游写数据
            ctx.collect(
                    new WaterSensor(
                            "sensor_" + random.nextInt(3),
                            System.currentTimeMillis(),
                            random.nextInt(10)+40
                    )
            );
            Thread.sleep(2000);
        }
    }

    @Override
    public void cancel() {
        this.flag = false;
    }
}
}
