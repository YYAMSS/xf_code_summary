package indicators;

import bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 0:50
 * @Version 1.0
 */
/*
* TODO 用sum的方式求和
* */
public class Flink_Project_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] s = line.split(" ");
                    return new UserBehavior(
                            Long.valueOf(s[0]),
                            Long.valueOf(s[1]),
                            Integer.valueOf(s[2]),
                            s[3],
                            Long.valueOf(s[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior()))
                .map(new MapFunction<UserBehavior, Tuple2>() {
                    @Override
                    public Tuple2 map(UserBehavior value) throws Exception {
                        return new Tuple2(value,1L);
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
