package operator.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:34
 * @Version 1.0
 */
public class FLink_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 0.Kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink01_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new FlinkKafkaConsumer<>("sensor", new SimpleStringSchema(), properties))
                .print("kafka source");

        env.execute();


    }
}
