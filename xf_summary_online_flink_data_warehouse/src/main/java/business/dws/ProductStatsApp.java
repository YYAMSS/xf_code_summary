package business.dws;

import base.FlinkAbstractBase;
import bean.OrderWide;
import bean.PaymentWide;
import bean.ProductStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import context.GmallConstant;
import functions.DimAsyncFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.validate.ValidationResult;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.joda.time.DateTimeUtils;
import tools.ClickHouseUtil;
import tools.DateTimeUtil;
import tools.MyKafkaUtil;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @Author xiefeng
 * @DATA 2021/10/4 13:24
 * @Version 1.0
 */
public class ProductStatsApp extends FlinkAbstractBase {
    @Override
    protected void transformation() throws Exception {

        //添加数据源
        DataStreamSource<String> pageViewDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_PAGE_LOG, GlobalContext.GROUP_ID));
        DataStreamSource<String> orderWideDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWM_ORDER_WIDE,GlobalContext.GROUP_ID));
        DataStreamSource<String> payWideDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWM_PAYMENT_WIDE,GlobalContext.GROUP_ID));
        DataStreamSource<String> cartDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_CART_INFO,GlobalContext.GROUP_ID));
        DataStreamSource<String> favorDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_FAVOR_INFO,GlobalContext.GROUP_ID));
        DataStreamSource<String> refundDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_ORDER_REFUND_INFO,GlobalContext.GROUP_ID));
        DataStreamSource<String> commentDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_COMMENT_INFO,GlobalContext.GROUP_ID));

        //将数据转换为统一的JavaBean ->ProductStats
        //该条流输出 点击和曝光 两种数据，所以用process
        SingleOutputStreamOperator<ProductStats> productStatsWithPageDS = pageViewDS.process(new ProcessFunction<String, ProductStats>() {
            @Override
            public void processElement(String value, Context ctx, Collector<ProductStats> out) throws Exception {
                //将数据转换为JSON对象
                JSONObject jsonObject = JSON.parseObject(value);
                //获取数据时间
                Long ts = jsonObject.getLong("ts");
                //获取页面信息
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");

                String itemType = page.getString("item_type");

                if ("good_detail".equals(pageId) && "sku_id".equals(itemType)) {
                    //取出被点击的商品ID
                    Long item = page.getLong("item");

                    out.collect(ProductStats.builder()
                            .sku_id(item)
                            .click_ct(1L)
                            .ts(ts)
                            .build());
                }

                //取出曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");

                //判断是否为曝光数据
                if(displays != null && displays.size() > 0){
                    for (int i = 0; i < displays.size(); i++) {
                        JSONObject displayJsonObj = displays.getJSONObject(i);
                        //取出当前曝光数据的数据类型
                        String item_type = displayJsonObj.getString("item_type");

                        //判断商品曝光数据
                        if("sku_id".equals(item_type)){
                            out.collect(ProductStats.builder()
                                    .sku_id(displayJsonObj.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build()
                            );
                        }
                    }
                }
            }
        });

        //3.2 收藏数据
        SingleOutputStreamOperator<ProductStats> productStatsWithFavorDS = favorDS.map(line -> {
            //将数据转换为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    //该时间来自于业务数据库，所以要将时间进行转化为时间戳
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //加购数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCartDS = cartDS.map(line -> {
            //将数据转化为json对象
            JSONObject jsonObject = JSON.parseObject(line);
            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(jsonObject.getLong("sku_num"))
                    .build();
        });

        //订单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithOrderDS = orderWideDS.map(line -> {
            //将数据转化为OrderWide
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            //创建集合用于存放订单ID，考虑去重
            HashSet<Object> hashSet = new HashSet<>();
            hashSet.add(orderWide.getOrder_id());

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    .order_sku_num(orderWide.getSku_num())
                    .order_amount(orderWide.getTotal_amount())
                    .orderIdSet(hashSet)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        //支付数据
        SingleOutputStreamOperator<ProductStats> productStatsWithPayDS = payWideDS.map(line -> {

            //将数据转化为PaymentWide
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            //创建集合用于存放订单ID
            HashSet<Object> hashSet = new HashSet<>();
            hashSet.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    .payment_amount(paymentWide.getTotal_amount())
                    .paidOrderIdSet(hashSet)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        //退单数据
        SingleOutputStreamOperator<ProductStats> productStatsWithRefundDS = refundDS.map(line -> {

            //将数据转化为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);

            //创建集合用于存放顶单ID
            HashSet<Object> hashSet = new HashSet<>();
            hashSet.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(hashSet)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //评价数据
        SingleOutputStreamOperator<ProductStats> productStatsWithCommentDS = commentDS.map(line -> {

            //将数据转化为Json对象
            JSONObject jsonObject = JSON.parseObject(line);

            //获取评价的数据类型
            String appraise = jsonObject.getString("appraise");
            Long goodCt = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(appraise)) {
                goodCt = 1L;
            }

            //封装对象并返回
            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .comment_ct(1L)
                    .good_comment_ct(goodCt)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        //todo 将各个流union到一起
        DataStream<ProductStats> unionDS = productStatsWithPageDS.union(
                productStatsWithFavorDS,
                productStatsWithCartDS,
                productStatsWithOrderDS,
                productStatsWithPayDS,
                productStatsWithRefundDS,
                productStatsWithCommentDS);

        //todo 提取时间戳生成watermark
        SingleOutputStreamOperator<ProductStats> productStatsWithWmDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy.
                <ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats element, long recordTimestamp) {
                        return element.getTs();
                    }
                })
        );

        //todo 分组、开窗、聚合
        KeyedStream<ProductStats, Long> keyStream = productStatsWithWmDS.keyBy(ProductStats::getSku_id);
        SingleOutputStreamOperator<ProductStats> reduceDS = keyStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                        //曝光数
                        value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                        //点击数
                        value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                        //加购数
                        value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                        //收藏数
                        value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                        //下单商品金额
                        value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                        //订单数量
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                        //下单商品个数
                        value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                        //退款支付订单数
                        value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                        value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() + 0L);
                        //退单金额
                        value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));
                        //评论订单数
                        value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                        value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);
                        //支付金额
                        value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));
                        //统计支付订单数
                        value1.setComment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                        //好评订单数
                        value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                        return value1;
                    }
                }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                        //取出数据
                        ProductStats productStats = input.iterator().next();

                        //处理窗口时间（开始、结束）
                        productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                        productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                        //处理订单数量
                        productStats.setOrder_ct(productStats.getOrderIdSet().size() + 0L);
                        //处理支付订单数量
                        productStats.setPaid_order_ct(productStats.getPaidOrderIdSet().size() + 0L);
                        //处理退款订单数量
                        productStats.setRefund_order_ct(productStats.getRefundOrderIdSet().size() + 0L);

                        //输出数据
                        out.collect(productStats);
                    }
                });


        //TODO 关联维度数据
        //关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SkU_INFO") {
                    @Override
                    public String getId(ProductStats input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) {

                        //取出维度信息
                        BigDecimal price = dimInfo.getBigDecimal("PRICE");
                        String sku_name = dimInfo.getString("SKU_NAME");
                        Long spu_id = dimInfo.getLong("SPU_ID");
                        Long tm_id = dimInfo.getLong("TM_ID");
                        Long category3_id = dimInfo.getLong("CTEGORY3_ID");

                        //将信息写入productStats对象
                        input.setSku_price(price);
                        input.setSku_name(sku_name);
                        input.setSpu_id(spu_id);
                        input.setTm_id(tm_id);
                        input.setCategory3_id(category3_id);

                    }
                },
                60,
                TimeUnit.SECONDS);
        //关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getId(ProductStats input) {
                        return input.getSpu_id().toString();

                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) {
                        String spu_name = dimInfo.getString("SPU_NAME");
                        input.setSpu_name(spu_name);
                    }
                }, 60, TimeUnit.SECONDS);

        //关联TradeMark维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getId(ProductStats input) {
                        return input.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) {

                        String tm_name = dimInfo.getString("TM_NAME");
                        input.setTm_name(tm_name);
                    }
                }, 60, TimeUnit.SECONDS);

        //关联Category维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithTmDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getId(ProductStats input) {
                        return String.valueOf(input.getCategory3_id());
                    }

                    @Override
                    public void join(ProductStats input, JSONObject dimInfo) {

                        String name = dimInfo.getString("NAME");
                        input.setCategory3_name(name);
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 写入ClickHouse
        productStatsWithCategory3DS.print();
        productStatsWithCategory3DS
                .addSink(ClickHouseUtil.getSinkFunc(
                        "insert into product_stats_201109 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));



    }
}
