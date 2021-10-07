package indicators;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 2:35
 * @Version 1.0
 */

import bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;
import java.util.ArrayList;

/**
 * todo 恶意登录监控
 * 对于网站而言，用户登录并不是频繁的业务操作。
 * 	如果一个用户短时间内频繁登录失败，就有可能是出现了程序的恶意攻击，比如密码暴力破解。
 * 	因此我们考虑，应该对用户的登录失败动作进行统计，
 * 	具体来说，如果同一用户（可以是不同IP）在2秒之内连续两次登录失败，就认为存在恶意登录的风险，
 * 	输出相关的信息进行报警提示。这是电商网站、也是几乎所有网站风控的基本一环。
 * */

public class Flink_Project_Login {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 创建WatermarkStrategy
        WatermarkStrategy<LoginEvent> wms = WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });
        env
                .readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new LoginEvent(Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000L);
                })
                .assignTimestampsAndWatermarks(wms)
                // 按照用户id分组
                .keyBy(LoginEvent::getUserId)
                .process(new KeyedProcessFunction<Long, LoginEvent, String>() {

                    private ListState<Long> failTss;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        failTss = getRuntimeContext().getListState(new ListStateDescriptor<Long>("failTss", Long.class));
                    }

                    @Override
                    public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
                    /*
                        统计连续失败的次数:
                            1. 把失败的时间戳放入到List中,
                            2. 当List中的长度到达2的时候, 判断这个两个时间戳的差是否小于等于2s
                            3. 如果是, 则这个用户在恶意登录
                            4. 否则不是, 然后删除List的第一个元素用于保持List的长度为2
                            6. 如果出现登录成功, 则需要清空List集合, 重新开始计算
                     */
                        switch (value.getEventType()) {
                            case "fail":
                                // 1. 把时间戳放入到集合中
                                failTss.add(value.getEventTime());
                                // 2. 把状态中的元素转存到ArrayList中
                                ArrayList<Long> tss = new ArrayList<>();
                                for (Long ts : failTss.get()) {
                                    tss.add(ts);
                                }
                                // 3. 如果长度等于2, 判断2次失败的时间是否在2秒以内
                                // 3.1 如果是则报警
                                // 3.2 否则, 应该删除第一条数据删除
                                if (tss.size() == 2) {
                                    long delta = tss.get(1) - tss.get(0);
                                    if (delta / 1000 <= 2) {
                                        out.collect(value.getUserId() + " 在恶意登录, 请注意!!!");
                                    } else {
                                        tss.remove(0);
                                        failTss.update(tss);
                                    }
                                }
                                break;
                            case "success":
                                failTss.clear();
                                break;
                            default:

                        }
                    }
                })
                .print();

        env.execute();
    }
}