package operator.transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import sun.plugin2.jvm.CircularByteBuffer;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:47
 * @Version 1.0
 */
public class Flink_Transform_flatMap {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 新的流存储每个元素的平方和3次方
        env.fromElements(1, 2, 3, 4, 5).flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                out.collect(value * value);
                out.collect(value * value * value);
            }
        }).print();

        //Lambda表达式写法
                env.fromElements(1, 2, 3, 4, 5)
                .flatMap((Integer value, Collector<Integer> out) -> {
                    out.collect(value * value);
                    out.collect(value * value * value);
                }).returns(Types.INT)
                .print();

                /*
                * 说明: 在使用Lambda表达式表达式的时候, 由于泛型擦除的存在,
                * 在运行的时候无法获取泛型的具体类型, 全部当做Object来处理, 及其低效,
                * 所以Flink要求当参数中有泛型的时候, 必须明确指定泛型的类型.
                * */

    }
}
