package operator.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:53
 * @Version 1.0
 */
public class Flink_Transform_KeyBy {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //匿名内部类写法
        // 奇数分一组, 偶数分一组
                env.fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 == 0 ? "偶数" : "奇数";
                    }
                })
                .print();

        //Lambda表达式写法
                env.fromElements(10, 3, 5, 9, 20, 8)
                .keyBy(value -> value % 2 == 0 ? "偶数" : "奇数")
                .print();

        env.execute();

    }
}
