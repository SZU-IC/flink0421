package charpter05;

import bean.MarketingUserBehavior;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
  * @Author:Wither
  * @Description:不同渠道的统计行为
  * @Date:15:38 2020/9/20
  */
public class Flink27_Case_APPMarketingAnalysis {
    public static void main(String[] args) {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取数据、转换成 bean 对象
        //env.addSource();
    }

    public static class AppSource implements SourceFunction<MarketingUserBehavior>{

        private boolean flag = true;
        private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");
        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (flag){
                final Random random = new Random();
                ctx.collect(
                        new MarketingUserBehavior(
                                Long.valueOf(random.nextInt(10)),
                                behaviorList.get(random.nextInt(behaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
