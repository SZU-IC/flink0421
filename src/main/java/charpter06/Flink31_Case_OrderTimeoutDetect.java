package charpter06;

import bean.OrderEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.lang.model.element.VariableElement;

/**
  * @Author:Wither
  * @Description:订单支付实时监控
  * @Date:19:42 2020/9/24
  */
public class Flink31_Case_OrderTimeoutDetect {
    public static void main(String[] args) {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1.读取数据，转换成bean对象
        final SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        final String[] datas = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent element) {
                                return element.getEventTime() * 1000L;
                            }
                        }
                );

        // TODO 2.处理数据：订单超时监控
        // TODO 2.1 按照 统计维度 分组：订单
        final KeyedStream<OrderEvent, Long> orderKS = orderEventDS.keyBy(data -> data.getOrderId());
        //TODO 2.2 超时分析
        final SingleOutputStreamOperator<String> resultDS = orderKS.process(new OrderTimeoutDetect());
        resultDS.print("result");

        OutputTag<String> outputTag = new OutputTag<String>("timeout"){};

        resultDS.getSideOutput(outputTag).print("timeout");

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class OrderTimeoutDetect extends KeyedProcessFunction<Long,OrderEvent,String>{
        private ValueState<OrderEvent> payState;
        private ValueState<OrderEvent> orderState;
        //TODO 记录第一条数据到的时间
        private ValueState<Long> timeoutTs;
        OutputTag timeoutTag = new OutputTag<String>("timeout"){};
        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("payState", OrderEvent.class,null));
            orderState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("orderState" ,OrderEvent.class,null));
            timeoutTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeoutTag", Long.class,0L));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //TODO 判断是谁没来: 只对设置定时器的逻辑进行判断
            //TODO 如果是create没来 => 说明pay来过，说明payState不为空
            if(payState.value() != null){
                // create没来 => 异常情况，告警
                ctx.output(timeoutTag, "订单"+payState.value().getOrderId()+"有支付数据，但下单数据丢失，系统存在异常！！！");
                // 清空
                payState.clear();
            }
            // 如果是pay没来 => 说明create来过，说明createState不为空
            if (orderState.value() != null){
                // pay没来 => 没支付
                ctx.output(timeoutTag,"订单"+orderState.value().getOrderId()+"支付超时！！！" );
                // 清空
                orderState.clear();
            }
            timeoutTs.clear();
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {

            //TODO 还要考虑:只来一个数据的情况
            if (timeoutTs.value() == null){
                //TODO 这里不用考虑来的是 create 还是 pay，统计都等 15 分钟
                //TODO ctx.timestamp() ：是时间戳
                ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 15 * 60 * 1000L);
                timeoutTs.update(ctx.timestamp() + 15 * 60 * 1000L);
            }else{
                // 说明另一条数据来了，删除定时器
                ctx.timerService().deleteEventTimeTimer(timeoutTs.value());
                timeoutTs.clear();
            }

            //数据可能是乱序的，对于同一个订单而言，可能是 pay 的数据先到
            //判断当前来的是什么数据
            if("create".equals(value.getEventType())){
                //1.说明当前的数据是 create => 判断 pay 是否来过
                if(payState.value() == null){
                    //pay 没有来过 => 把 create 存起来
                    orderState.update(value);
                }else {
                    if(payState.value().getEventTime() - value.getEventTime() > 15 * 60){
                        // 超时，发出告警
                        ctx.output(timeoutTag,  "订单" + value.getOrderId() + "支付成功，但是超时，系统可能存在漏洞，请及时修复！！！");
                    }else {
                        // 1.2.2 没超时
                        out.collect("订单" + value.getOrderId() + "支付成功！");
                    }
                    //使用完，清空
                    payState.clear();
                }
            }else {
                // 2. 说明当前的数据是 paystate => 判断create 是否来过
                if(orderState.value() == null){
                    // 2.1 说明create 没有来过 => 把当前的 pay 存起来
                    payState.update(value);
                }else {
                    // 2.2 说明 create 来过，判断是否超时
                    if((value.getEventTime() - orderState.value().getEventTime()) > 15 * 60){
                        // 超时
                        ctx.output(timeoutTag, "订单" + value.getOrderId() + "支付成功，但是超时，系统可能存在漏洞，请及时修复！！！");
                    }else {
                        // 2.2.2 没超时,往下游写
                        out.collect("订单" + value.getOrderId() + "支付成功！");
                    }
                    //使用完，清空
                    orderState.clear();
                }
            }
        }
    }
}
