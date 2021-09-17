package business.ods;

import base.FlinkAbstractBase;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import functions.MyDeserializerFunc;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import tools.MyKafkaUtil;

/**
 * @Author xiefeng
 * @DATA 2021/9/13 15:23
 * @Version 1.0
 */
public class FlinkCDCAPI extends FlinkAbstractBase {

    @Override
    protected void transformation() throws Exception {

        DebeziumSourceFunction<String> sourceFunction = MySQLSource
                .<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("xxff")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserializerFunc())
                .build();
        DataStreamSource<String> dataStreamSource = this.env.addSource(sourceFunction);

        String topic = "ods_xf_test_db";
        dataStreamSource.addSink(MyKafkaUtil.getFlinkKafkaProducer(topic));
    }
}



