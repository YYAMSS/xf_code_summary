package business.dws;

import base.FlinkAbstractBase;
import bean.VisitorStats;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tools.ClickHouseUtil;
import tools.DateTimeUtil;
import tools.MyKafkaUtil;

import java.time.Duration;
import java.util.Date;

/**
 * @Author xiefeng
 * @DATA 2021/10/4 13:25
 * @Version 1.0
 */

/**
 * 1、接入page、uv、uj 主题；
 * 2、重新设置消费者组；
 * 3、page主题用于求PV、UV主题用于求uv、uj主题用于求跳出数据；
 * 4、将各个流转化为相同的数据格式，最后再union在一起；
 * 5、最后再进行分组、开窗、聚合；
 * 6、最终再输出到CK;
 * */
public class VisitorStatsApp extends FlinkAbstractBase {
    @Override
    protected void transformation() throws Exception {
        //接入数据源
        DataStreamSource<String> pageDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWD_PAGE_LOG, GlobalContext.GROUP_ID));
        DataStreamSource<String> uvDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWM_UNIQUE_VISIT, GlobalContext.GROUP_ID));
        DataStreamSource<String> ujDS = this.env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(GlobalContext.DWM_USER_JUMP_DETAIL, GlobalContext.GROUP_ID));

        //将各个流转化为统一的格式
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithPvSvDt = pageDS.map(line -> {
            //将数据转化为JSON对象
            JSONObject jsonObject = JSON.parseObject(line);
            //取出公共字段
            JSONObject common = jsonObject.getJSONObject("common");
            //取出上一条的页面信息
            String lastPage = jsonObject.getJSONObject("page").getString("last_page_id");

            Long sv = 0L;
            if (lastPage == null || lastPage.length() <= 0) {
                sv = 1L;
            }

            return new VisitorStats("",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    1L,
                    sv,
                    0L,
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUv = uvDS.map(line -> {

            JSONObject jsonObject = JSON.parseObject(line);

            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        SingleOutputStreamOperator<VisitorStats> visitorStatsWithUj = ujDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("",
                    "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        //TODO union各个流 ， 并提取时间戳
        SingleOutputStreamOperator<VisitorStats> unionDS = visitorStatsWithPvSvDt
                .union(visitorStatsWithUv, visitorStatsWithUj)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));

        //TODO 按照维度信息分组
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStream = unionDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                return new Tuple4<>(value.getVc(),
                        value.getCh(),
                        value.getAr(),
                        value.getIs_new());
            }
        });

        //TODO 开窗、聚合
        SingleOutputStreamOperator<VisitorStats> result = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                        value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                        value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                        value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                        value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());
                        value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                        return value1;
                    }
                }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                        //提取数据
                        VisitorStats visitorStats = input.iterator().next();

                        //补充窗口时间字段
                        long start = window.getStart();
                        long end = window.getEnd();

                        String stt = DateTimeUtil.toYMDhms(new Date(start));
                        String edt = DateTimeUtil.toYMDhms(new Date(end));

                        visitorStats.setStt(stt);
                        visitorStats.setEdt(edt);

                        out.collect(visitorStats);
                    }
                });

        //TODO 将数据写入ClickHouse
        result.print(">>>>>>>");
        result.addSink(ClickHouseUtil.getSinkFunc("insert into visitor_stats_201109 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

    }
}
