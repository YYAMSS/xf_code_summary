package indicators;

import bean.UserBehavior;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 0:59
 * @Version 1.0
 */
/*
* TODO 用 process 方式求和
* */
public class Flink_Project_PV2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] s = line.split(" ");
                    return new UserBehavior(
                            Long.valueOf(s[1]),
                            Long.valueOf(s[2]),
                            Integer.valueOf(s[3]),
                            s[4],
                            Long.valueOf(s[5])
                    );
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior()))
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    long count = 0;
                    @Override
                    public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                        count++;
                        out.collect(count);
                    }
                })
                .print();
        env.execute();
    }
}
