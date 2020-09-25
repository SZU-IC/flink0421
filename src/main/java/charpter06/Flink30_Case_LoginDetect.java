package charpter06;

import bean.LoginEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.lang.model.element.VariableElement;

/**
 * @author Wither
 * 2020/9/24
 * charpter06
 */
public class Flink30_Case_LoginDetect {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1.读取数据
        final SingleOutputStreamOperator<LoginEvent> loginDS = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new LoginEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(10)) {
                            @Override
                            public long extractTimestamp(LoginEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        //处理数据
        //TODO 2秒之内连续两次登录失败
        final SingleOutputStreamOperator<LoginEvent> filterDS = loginDS.filter(data -> "fail".equals(data.getEventType()));
        //TODO 按照维度进行分组 ：用户
        final KeyedStream<LoginEvent, Long> loginKS = filterDS.keyBy(data -> data.getUserId());
        //TODO 分析是否为恶意登录
        loginKS.process(new LoginFailDetect()).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class LoginFailDetect extends KeyedProcessFunction<Long,LoginEvent,LoginEvent> {

        private ValueState<LoginEvent> datas;
        OutputTag outputTag = new OutputTag<String>("failDetect");

        @Override
        public void open(Configuration parameters) throws Exception {
            datas = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("datas", LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<LoginEvent> out) throws Exception {
            //判断datas是否为空，为空则是第一次失败则加入
            if (datas.value() == null) {
                datas.update(value);
            } else {
                if (Math.abs(ctx.timestamp() - datas.value().getEventTime()) <= 2) {
                    ctx.output(outputTag, datas.value().getUserId() + "在两秒之内失败登录两次");

                }
                datas.update(datas.value());
            }
            out.collect(datas.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginEvent> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }
}
