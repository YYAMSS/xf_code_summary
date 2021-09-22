package business.dwd;

import base.FlinkAbstractBase;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import functions.DbSplitTableJDBC;
import functions.DimSink;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import tools.MyKafkaUtil;

import javax.annotation.Nullable;

/**
 * @Author xiefeng
 * @DATA 2021/9/12 1:33
 * @Version 1.0
 */
public class DwdDbSplitJDBC extends FlinkAbstractBase {


    @Override
    protected void transformation() throws Exception {

        System.out.println("flink transformation this stream start!");

        //-------------配置Kafka相关参数---------------
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(
                this.config.getString(GlobalContext.KAFKA_TOPIC_ODS),
                this.config.getString(GlobalContext.KAFKA_GROUP_ID),
                this.config.getString(GlobalContext.KAFKA_OFFSET));
        DataStreamSource<String> kafkaDataStreamSource = this.env.addSource(kafkaSource);

        //kafkaDataStreamSource.print("kafka>>>>>>>");
        //-------------数据格式转化---------------------
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDataStreamSource.map(JSON::parseObject);
        //-------------过滤数据-------------------------
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String data = value.getString("data");
                return data != null && data.length() > 0;
            }
        });

        filterDS.print("filterDS<<<<<<<<<<<");

        //-------------分流---------------------
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = filterDS.process(new DbSplitTableJDBC(hbaseTag));

        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        kafkaJsonDS.print("kafka<<<<<<<<");

        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getkafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("开始序列化Kafka数据！");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(element.getString("sink_table"),
                        element.getString("data").getBytes());
            }
        });

        kafkaJsonDS.addSink(kafkaSinkBySchema);

        hbaseJsonDS.print("hbase<<<<<<<<");
        hbaseJsonDS.addSink(new DimSink());

    }
}
