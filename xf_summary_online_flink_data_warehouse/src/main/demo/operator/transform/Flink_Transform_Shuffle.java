package operator.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:54
 * @Version 1.0
 */
public class Flink_Transform_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(10, 3, 5, 9, 20, 8)
                .shuffle()
                .print();
        env.execute();

    }
}
