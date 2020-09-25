package charpter05;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
  * @Author:Wither
  * @Description:PV的统计
  * @Date:23:04 2020/9/18
  */
public class Flink23_Case_PV {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        final StreamExecutionEnvironment senv =
                StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);

        //1. 从文件读取数据、转换成 bean 对象
        final SingleOutputStreamOperator<UserBehavior> inputDS = senv.readTextFile("input/UserBehavior.csv")
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

        //TODO 参考WordCount思路，实现 PV 处理
        // 2. 处理数据
        // 2.1 过滤出 PV 行为
        final SingleOutputStreamOperator<Tuple2<String, Integer>> pvAndOnrTuple2 = inputDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                //return Tuple2.of("pv", 1);
                return new Tuple2<>("pv",1);
            }
        });

        // 2.3 按照第一个位置的元素 分组 =》 聚合算子只能在分组之后调用，也就是 keyedStream才能调用 sum
        final KeyedStream<Tuple2<String, Integer>, Tuple> pvAndOneKS = pvAndOnrTuple2.keyBy(0);

        // 2.4 求和
        final SingleOutputStreamOperator<Tuple2<String, Integer>> pvDS = pvAndOneKS.sum(1);

        // 3.打印
        pvDS.print("pv");

        senv.execute();
    }
}
