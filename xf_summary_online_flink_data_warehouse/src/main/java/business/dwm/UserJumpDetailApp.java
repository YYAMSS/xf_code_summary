package business.dwm;

import base.FlinkAbstractBase;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.SimpleTimerService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import tools.MyKafkaUtil;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author xiefeng
 * @DATA 2021/10/3 12:11
 * @Version 1.0
 */

/*
* 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。
* 而跳出率就是用跳出次数除以访问次数。
* */
public class UserJumpDetailApp extends FlinkAbstractBase {
    @Override
    protected void transformation() throws Exception {
        //TODO 接入数据源
        DataStreamSource<String> kafkaDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_PAGE_LOG, GlobalContext.GROUP_ID));
        //TODO 将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> mapDS = kafkaDS.map(JSON::parseObject);
        //TODO 定义水位线等
        WatermarkStrategy<JSONObject> watermarkStrategy = WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");

                    }
                });
        SingleOutputStreamOperator<JSONObject> jsonObjWithWM = mapDS.assignTimestampsAndWatermarks(watermarkStrategy);
        //TODO 按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWM.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 定义模式序列
        //两次判断10秒内的上一条数据是否为空，匹配上（肯定为跳出）的和超时（只跳出了一次）的数据都要；
        //一个条件事件+一个超时事件的组合；
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        //提取数据中上一跳的页面:
                        // 这个可以通过该页面是否有上一个页面（last_page_id）来判断，
                        // 如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");
                        return lastPageId == null || lastPageId.length() <= 0;

                    }
                }).times(2)  //默认为非严格近邻
                .consecutive()  //转为严格近邻
                .within(Time.seconds(30)); //设置超时间为30s

        //TODO 将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        //TODO 提取事件(包含超时事件)
        OutputTag<String> outputTag = new OutputTag<String>("TimeOut") {
        };

        SingleOutputStreamOperator<String> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, String>() {
            //处理超时事件
            @Override
            public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {

                JSONObject start = pattern.get("start").get(0);
                return start.toJSONString();

            }
        }, new PatternSelectFunction<JSONObject, String>() {
            //处理匹配上的数据
            @Override
            public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                JSONObject start = pattern.get("start").get(0);
                return start.toJSONString();
            }
        });

        //TODO 合并主流（匹配上的事件） 和 测输出流（超时事件）
        DataStream<String> sideOutput = selectDS.getSideOutput(outputTag);
        DataStream<String> result = selectDS.union(sideOutput);

        //TODO 写入kafka
        result.addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWM_USER_JUMP_DETAIL));
    }
}
