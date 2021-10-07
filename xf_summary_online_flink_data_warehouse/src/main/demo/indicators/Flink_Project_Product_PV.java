package indicators;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 2:04
 * @Version 1.0
 */
/**
 * todo 指定时间范围内网站总浏览量（PV）的统计
 *  实现一个网站总浏览量的统计。我们可以设置滚动时间窗口，实时统计每小时内的网站PV。
 *  此前我们已经完成了该需求的流数据操作，当前需求是在之前的基础上增加了窗口信息
* */
import bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;

/*
* TODO  添加watermark ，
*       使用窗口函数 确定时间段，
*       使用sum() 求和；
* */

public class Flink_Project_Product_PV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建WatermarkStrategy
        WatermarkStrategy<UserBehavior> wms = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        env.readTextFile("input/UserBehavior.csv")
                .map(line -> { // 对数据切割, 然后封装到POJO中
                    String[] split = line.split(",");
                    return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
                })
                .filter(behavior -> "pv".equals(behavior.getBehavior())) //过滤出pv行为
                .assignTimestampsAndWatermarks(wms)  // 添加 Watermark
                .map(behavior -> Tuple2.of("pv", 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG)) // 使用Tuple类型, 方便后面求和
                .keyBy(value -> value.f0)  // keyBy: 按照key分组
                .window(TumblingEventTimeWindows.of(Time.minutes(60)))  // 分配窗口
                .sum(1) // 求和
                .print();
        env.execute();
    }
}