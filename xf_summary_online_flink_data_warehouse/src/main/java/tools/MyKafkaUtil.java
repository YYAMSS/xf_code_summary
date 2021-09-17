package tools;

import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author xiefeng
 * @DATA 2021/9/12 13:24
 * @Version 1.0
 */
public class MyKafkaUtil {
    private static Properties properties = new Properties();
    static{
        properties.put(GlobalContext.BOOTSTRAP_SERVERS,GlobalContext.KAFKA_BOOTSTRAP_SERVERS);
    }

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId,String offset){
        properties.put(GlobalContext.GROUP_ID,groupId);
        properties.put(GlobalContext.AUTO_OFFSET_RESET,offset);

        return new FlinkKafkaConsumer<>(topic,new SimpleStringSchema(),properties);
    }

    /**
     * 获取生产者对象
     *
     * @param topic 主题
     */
    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {

        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);

    }

    public static <T> FlinkKafkaProducer<T> getkafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,5 * 60 * 1000 + "");
        return new FlinkKafkaProducer<T>("kafka_default_topic",
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

    }
}



