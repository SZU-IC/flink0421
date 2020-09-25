package charpter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
public class Flink24_Case_PVByProcess {
    public static void main(String[] args) {
        //创建执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        // 1.从文件读取是数据、转换成 bean 对象
        final SingleOutputStreamOperator<UserBehavior> userBehaviorDS = senv.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });

        // TODO 实现PV的统计
        // 2.处理数据
        // 2.1 过滤出 PV 行为
        final SingleOutputStreamOperator<UserBehavior> userBehaviorFilter = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));
        //2.2 按照 统计的维度 分组 ：PV 行为
        final KeyedStream<UserBehavior, String> userBehaviorKS
                = userBehaviorFilter.keyBy(data -> data.getBehavior());
        //2.3 求和 =》 实现 计数 的功能，没有count这种举和算子
        //一般找不到现成的算子，那就调用底层的 process
        final SingleOutputStreamOperator<Long> resultDS = userBehaviorKS.process(
                new KeyedProcessFunction<String, UserBehavior, Long>() {
                    private Long pvCount = 0L;

                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        //定义一个变量，来统计条数
                        //来一条 pvCount++
                        pvCount++;
                        //采集器往下游发送统计结果
                        out.collect(pvCount);


                    }
                }
        );

        resultDS.print("pv by process");
        try {
            senv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
