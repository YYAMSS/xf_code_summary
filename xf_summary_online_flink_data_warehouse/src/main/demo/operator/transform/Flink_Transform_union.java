package operator.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:57
 * @Version 1.0
 */
public class Flink_Transform_union {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> stream1 = env.fromElements(1, 2, 3, 4, 5);
        DataStreamSource<Integer> stream2 = env.fromElements(10, 20, 30, 40, 50);
        DataStreamSource<Integer> stream3 = env.fromElements(100, 200, 300, 400, 500);

        // 把多个流union在一起成为一个流, 这些流中存储的数据类型必须一样: 水乳交融
        //stream1.union(stream2,stream3)
        stream1.union(stream2)
                .union(stream3)
                .print();
    }
}
/*
* todo connect与 union 区别：
1.union之前两个流的类型必须是一样，connect可以不一样
2.connect只能操作两个流，union可以操作多个。
* */