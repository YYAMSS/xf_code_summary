package business.dwm;

import base.FlinkAbstractBase;
import bean.OrderWide;
import bean.PaymentInfo;
import bean.PaymentWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import tools.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 21:49
 * @Version 1.0
 */
public class PaymentWideApp extends FlinkAbstractBase {


    @Override
    protected void transformation() throws Exception {

        DataStreamSource<String> orderWideStrDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWM_ORDER_WIDE, GlobalContext.GROUP_ID));
        DataStreamSource<String> paymentInfoStrDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_PAYMENT_INFO, GlobalContext.GROUP_ID));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {

                    @Override
                    public long extractTimestamp(OrderWide element, long recordTimestamp) {

                        String create_time = element.getCreate_time();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                        try {
                            return sdf.parse(create_time).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                            return recordTimestamp;
                        }

                    }
                }));

        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                String create_time = element.getCreate_time();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                                try {
                                    return sdf.parse(create_time).getTime();
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                    return recordTimestamp;
                                }
                            }
                        }));

        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                .between(Time.minutes(-15), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        paymentWideDS.print();
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getFlinkKafkaProducer(GlobalContext.DWM_PAYMENT_WIDE));

    }
}
