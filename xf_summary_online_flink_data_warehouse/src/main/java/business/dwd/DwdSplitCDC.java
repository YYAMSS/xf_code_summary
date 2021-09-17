package business.dwd;

import base.FlinkAbstractBase;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import context.GlobalContext;
import functions.DimSink;
import functions.MyDeserializerFunc;
import functions.TableProcessFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import tools.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import javax.annotation.Nullable;

/**
 * @Author xiefeng
 * @DATA 2021/9/17 0:55
 * @Version 1.0
 */
public class DwdSplitCDC extends FlinkAbstractBase {

    @Override
    protected void transformation() throws Exception {

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(
                this.config.getString(GlobalContext.KAFKA_TOPIC_ODS),
                this.config.getString(GlobalContext.KAFKA_GROUP_ID),
                this.config.getString(GlobalContext.KAFKA_OFFSET)
        );
        DataStreamSource<String> DataStreamSource = this.env.addSource(kafkaSource);
        DataStreamSource.print("source<<<<<<<<<");

        SingleOutputStreamOperator<JSONObject> mapDS = DataStreamSource.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> dataDS = mapDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String data = value.getString("data");
                return data != null && data.length() > 0;
            }
        });

        dataDS.print("dataDS<<<<<<");


        //添加配置表流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("xxff")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserializerFunc())
                .build();

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("bc-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectedStream = dataDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseOutPutTag = new OutputTag<JSONObject>("hbase") {
        };

        SingleOutputStreamOperator<JSONObject> connectDS = connectedStream.process(new TableProcessFunction(hbaseOutPutTag, mapStateDescriptor));

        connectDS.print("kafka<<<<<<<<");

        DataStream<JSONObject> hbaseDS = connectDS.getSideOutput(hbaseOutPutTag);
        hbaseDS.print("hbase<<<<<<<<<<");

        FlinkKafkaProducer<JSONObject> kafkaSinkBySchema = MyKafkaUtil.getkafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {

                return new ProducerRecord<byte[], byte[]>(element.getString("sinkTable"),
                        element.getString("data").getBytes());
            }
        });
        connectDS.addSink(kafkaSinkBySchema);

        hbaseDS.addSink(new DimSink());

    }
}
