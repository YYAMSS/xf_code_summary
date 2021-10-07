package operator.source;

import bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:12
 * @Version 1.0
 */
public class Flink_Source_Collection {
    public static void main(String[] args) throws Exception {
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("ws_001", 1577844001L, 45),
                new WaterSensor("ws_002", 1577844015L, 43),
                new WaterSensor("ws_003", 1577844020L, 42));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> wsDS = env.fromCollection(waterSensors);
        wsDS.print(">>>");
        env.execute();


    }

}
