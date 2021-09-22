package business.dwm;

import base.FlinkAbstractBase;
import bean.OrderDetail;
import bean.OrderInfo;
import bean.OrderWide;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import functions.DimAsyncFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import tools.MyKafkaUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @Author xiefeng
 * @DATA 2021/9/18 15:04
 * @Version 1.0
 */
public class OrderWideApp extends FlinkAbstractBase {


    @Override
    protected void transformation() throws Exception {
        FlinkKafkaConsumer<String> orderInfoSourceTopicSource = MyKafkaUtil.getKafkaSource(GlobalContext.DWD_ORDER_INFO, GlobalContext.KAFKA_GROUP_ID, GlobalContext.KAFKA_OFFSET);
        FlinkKafkaConsumer<String> orderDetailTopicSource = MyKafkaUtil.getKafkaSource(GlobalContext.DWD_ORDER_DETAIL, GlobalContext.KAFKA_GROUP_ID, GlobalContext.KAFKA_OFFSET);

        DataStreamSource<String> orderInfoSourceDS = this.env.addSource(orderInfoSourceTopicSource);
        DataStreamSource<String> orderDetailSourceDS = this.env.addSource(orderDetailTopicSource);

        //订单流
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWMDS = orderInfoSourceDS.map(line -> {
            OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

            //yyyy-MM-dd HH:mm:ss
            String create_time = orderInfo.getCreate_time();
            String[] dateHourArr = create_time.split(" ");

            orderInfo.setCreate_date(dateHourArr[0]);
            orderInfo.setCreate_hour(dateHourArr[1].split(":")[0]);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = sdf.parse(create_time).getTime();

            orderInfo.setCreate_ts(ts);
            return orderInfo;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
            @Override
            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));
        //订单明细流
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWMDS = orderDetailSourceDS.map(line -> {
            OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);

            String create_time = orderDetail.getCreate_time();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long ts = sdf.parse(create_time).getTime();

            orderDetail.setCreate_ts(ts);
            return orderDetail;
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {

            @Override
            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                return element.getCreate_ts();
            }
        }));

        //双流JOIN
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWMDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWMDS.keyBy(OrderDetail::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        //维度用户关联
        SingleOutputStreamOperator orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getId(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) {
                        String birthday = dimInfo.getString("BIRTHDAY");
                        String gender = dimInfo.getString("GENDER");

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        long ts = System.currentTimeMillis();
                        long time = ts;

                        try {
                            time = sdf.parse(birthday).getTime();
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        long age = (ts - time)/(1000L * 60 * 60 * 24 * 365);

                        input.setUser_age(Integer.parseInt(String.valueOf(age)));
                        input.setUser_gender(gender);
                    }
                }, 100, TimeUnit.SECONDS);




    }
}
