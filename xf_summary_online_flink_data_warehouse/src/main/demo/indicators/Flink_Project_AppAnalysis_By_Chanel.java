package indicators;

import bean.MarketingUserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 1:28
 * @Version 1.0
 */
/*
APP市场推广统计 - 分渠道
* 一般会通过各种不同的渠道对自己的APP进行市场推广，
* 而这些渠道的统计数据（比如，不同网站上广告链接的点击量、APP下载量）就成了市场营销的重要商业指标。

自定义数据源
* */
public class Flink_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //分渠道 keyBY时添加渠道维度
        env.addSource(new AppMarketingDataSource())
                .map(behavior -> Tuple2.of(behavior.getChannel() + "_" + behavior.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        //不分渠道
        env.addSource(new AppMarketingDataSource())
                .map(behavior -> Tuple2.of(behavior.getBehavior(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute();
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}
