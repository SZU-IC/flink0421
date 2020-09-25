package charpter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Wither
 * 2020/9/18
 * charpter05
 */
public class Flink26_Case_UV {
    public static void main(String[] args) {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件读取数据、转换成 bean对象
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {

                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(datas[0]),
                                Long.valueOf(datas[1]),
                                Integer.valueOf(datas[2]),
                                datas[3],
                                Long.valueOf(datas[4])
                        );
                    }
                });

        // TODO 参考wordcount思路，实现 PV 的统计
        // 2. 处理数据
        // 2.1 过滤 PV 行为
        final SingleOutputStreamOperator<UserBehavior> userBeahaviorFilter
                = userBehaviorDS.filter(data -> "pv".equals(data.getBehavior()));

        //转换成二元组
        final SingleOutputStreamOperator<Tuple2<String, Integer>> pvAndOneKeyTuple2 = userBeahaviorFilter.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });

        final KeyedStream<Tuple2<String, Integer>, String> userBehaviorKS = pvAndOneKeyTuple2.keyBy(r -> r.f0);

        final SingleOutputStreamOperator<Tuple2<String, Integer>> result = userBehaviorKS.sum(1);

        result.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
