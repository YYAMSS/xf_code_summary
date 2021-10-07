package operator.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.plugin2.jvm.CircularByteBuffer;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:51
 * @Version 1.0
 */
public class Flink_Transform_filter {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //匿名内部类写法
        // 保留偶数, 舍弃奇数
        env.fromElements(10, 3, 5, 9, 20, 8)
        .filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        })
        .print();

        //Lambda表达式写法
        env.fromElements(10, 3, 5, 9, 20, 8)
        .filter(value -> value % 2 == 0)
                .print();
    }
}
