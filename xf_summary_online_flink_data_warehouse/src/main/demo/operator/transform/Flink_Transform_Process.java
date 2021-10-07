package operator.transform;

import bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 23:11
 * @Version 1.0
 */

/*作用:process算子在Flink算是一个比较底层的算子,
        很多类型的流上都可以调用,可以从流中获取更多的信息(不仅仅数据本身)*/

public class Flink_Transform_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        //示例1: 在keyBy之前的流上使用
        env.fromCollection(waterSensors)
                .process(new ProcessFunction<WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>(value.getId(), value.getVc()));
                    }
                })
                .print();


        //示例2: 在keyBy之后的流上使用
        env.fromCollection(waterSensors)
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, Tuple2<String, Integer>>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(new Tuple2<>("key是:" + ctx.getCurrentKey(), value.getVc()));
                    }
                })
                .print();


        env.execute();

    }
}
