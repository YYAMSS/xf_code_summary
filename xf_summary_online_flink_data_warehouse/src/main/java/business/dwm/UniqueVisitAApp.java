package business.dwm;

import base.FlinkAbstractBase;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos;
import tools.MyKafkaUtil;

import java.text.SimpleDateFormat;

/**
 * @Author xiefeng
 * @DATA 2021/10/3 12:10
 * @Version 1.0
 */

/**
 * 1、接入页面数据流
 * 2、用状态编程的方式进行过滤，方便后续统计UV
 * */
public class UniqueVisitAApp extends FlinkAbstractBase {

    @Override
    protected void transformation() throws Exception {

        //TODO 添加数据源、更换新的groupID
        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_PAGE_LOG, GlobalContext.GROUP_ID);
        DataStreamSource<String> dataStreamSource = this.env.addSource(flinkKafkaConsumer);

        //TODO 将数据转换为JSON
        SingleOutputStreamOperator<JSONObject> jsonObject = dataStreamSource.map(JSON::parseObject);

        //TODO 将数据按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObject.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 使用状态编程的方式进行过滤
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 定义状态类型
                ValueStateDescriptor<String> stringValueStateDescriptor = new ValueStateDescriptor<>("last-vist", String.class);
                //TODO 设置状态失效时间
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stringValueStateDescriptor.enableTimeToLive(ttlConfig);
                //TODO 创建状态
                lastVisitState = getRuntimeContext().getState(stringValueStateDescriptor);

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //TODO 取出状态中的数
                String lastVisit = lastVisitState.value();
                //TODO 取出当前数据中的时间
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String curDate = sdf.format(value.getLong("ts"));

                if (lastVisit == null || !lastVisit.equals(curDate)) {
                    //TODO 今天的新访问用户
                    lastVisitState.update(curDate);
                    return true;
                }
                return false;
            }
        });

        uvDS.print();
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWM_UNIQUE_VISIT));
    }
}
