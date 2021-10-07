package operator.transform;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 23:00
 * @Version 1.0
 */
public class Flink_Transform_sum_min_max_minBy_maxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //示例1
        DataStreamSource<Integer> stream = env.fromElements(1, 2, 3, 4, 5);
        KeyedStream<Integer, String> kbStream = stream.keyBy(ele -> ele % 2 == 0 ? "奇数" : "偶数");
        kbStream.sum(0).print("sum");
        kbStream.max(0).print("max");
        kbStream.min(0).print("min");

        //示例2
        /*
        *分组聚合后, 理论上只能取分组字段和聚合结果, 但是Flink允许其他的字段也可以取出来,
        *其他字段默认情况是取的是这个组内第一个元素的字段值
        * */
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 30));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream2 = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream2
                .sum("vc")
                .print("maxBy...");

        //示例3
        /*
        注意: maxBy和minBy可以指定当出现相同值的时候,其他字段是否取第一个.
	          true表示取第一个, false表示取最后一个.
        * */
        ArrayList<WaterSensor> waterSensors1 = new ArrayList<>();
        waterSensors1.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors1.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors1.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors1.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors1.add(new WaterSensor("sensor_2", 1607527995000L, 30));

        KeyedStream<WaterSensor, String> kbStream3 = env
                .fromCollection(waterSensors)
                .keyBy(WaterSensor::getId);

        kbStream3
                .maxBy("vc", false)
                .print("max...");

        env.execute();

    }
}
