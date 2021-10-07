package indicators;

import bean.UserBehavior;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 1:25
 * @Version 1.0
 */
/*
* TODO 使用 hashMap方式 去重
* */
public class Flink_Project_UV {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/UserBehavior.csv")
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(",");
                    UserBehavior behavior = new UserBehavior(
                            Long.valueOf(split[0]),
                            Long.valueOf(split[1]),
                            Integer.valueOf(split[2]),
                            split[3],
                            Long.valueOf(split[4]));
                    if ("pv".equals(behavior.getBehavior())) {
                        out.collect(Tuple2.of("uv", behavior.getUserId()));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    HashSet<Long> userIds = new HashSet<>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        userIds.add(value.f1);
                        out.collect(userIds.size());
                    }
                })
                .print("uv");
        env.execute();
    }
}
