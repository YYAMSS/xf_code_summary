package business.dwd;

import base.FlinkAbstractBase;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.sun.org.apache.bcel.internal.generic.NEW;
import context.GlobalContext;
import org.apache.calcite.avatica.proto.Common;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tools.MyKafkaUtil;

/**
 * @Author xiefeng
 * @DATA 2021/9/23 18:03
 * @Version 1.0
 */
public class DWDBaseLogApp extends FlinkAbstractBase {

    @Override
    protected void transformation() throws Exception {
        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.ODS_BASE_LOG, GlobalContext.GROUP_ID);
        DataStreamSource<String> logStreamSource = this.env.addSource(flinkKafkaConsumer);

        OutputTag<String> dirty = new OutputTag<String>("DirtyData") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = logStreamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    e.printStackTrace();
                    ctx.output(dirty, value);
                }
            }
        });
        //TODO 按照设备ID分组、使用状态编程做新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlag = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<String> isNewState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //定义状态；
                        isNewState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("isNew-state", String.class));

                    }

                    @Override
                    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                        //取出数据中的is_new字段；
                        String isNew = value.getJSONObject("common").getString("is_new");
                        //如果is_new为1，则需要继续校验；
                        if ("1".equals(isNew)) {
                            //取出状态中的数据，并校验是否为null;
                            if (isNewState.value() != null) {
                                //说明现在mid不是新用户，修改mid的值;
                                value.getJSONObject("common").put("is_new", "0");
                            } else {
                                //说明为真正的新用户
                                isNewState.update("0");
                            }
                        }

                        out.collect(value);
                    }
                });

        OutputTag<String> startOutPutTag = new OutputTag<String>("Start") {
        };

        OutputTag<String> displayOutPutTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                String start = value.getString("start");
                if (start != null && start.length() > 0) {
                    //为启动数据
                    ctx.output(startOutPutTag, value.toJSONString());
                } else {
                    //不是启动数据，这一定为页面数据
                    out.collect(value.toJSONString());

                    //获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    //取出公共字段、页面信息、事件戳
                    JSONObject common = value.getJSONObject("common");
                    JSONObject page = value.getJSONObject("page");
                    Long ts = value.getLong("ts");

                    if (displays != null && displays.size() > 0) {
                        JSONObject displayObj = new JSONObject();
                        displayObj.put("common", common);
                        displayObj.put("page", page);
                        displayObj.put("ts", ts);

                        //遍历每一个曝光数据
                        for (Object display : displays) {
                            displayObj.put("display", display);
                            //输出到到输出流
                            ctx.output(displayOutPutTag, displayObj.toJSONString());
                        }
                    }
                }
            }
        });

        //测试
        jsonObjDS.getSideOutput(dirty).print("dirty<<<<<<<<<<<<");
        pageDS.getSideOutput(startOutPutTag).print("start<<<<<<<<");
        pageDS.getSideOutput(displayOutPutTag).print("display<<<<<<<<<<<");

        //3个测输出流+1主流，分别发往kafka;
        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWD_PAGE_LOG));
        jsonObjDS.getSideOutput(dirty).addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWD_DIRTY_LOG));
        pageDS.getSideOutput(startOutPutTag).addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWD_START_LOG));
        pageDS.getSideOutput(displayOutPutTag).addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWD_DISPLAY_LOG));
    }
}
