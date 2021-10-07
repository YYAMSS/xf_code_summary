package indicators;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 2:14
 * @Version 1.0
 */
/*
* todo
*  我们在这里先实现“热门页面浏览数”的统计，也就是读取服务器日志中的每一行log，
*  统计在一段时间内用户访问每一个url的次数，然后排序输出显示。
*  具体做法为：每隔5秒，输出最近10分钟内访问量最多的前N个URL。
*  可以看出，这个需求与之前“实时热门商品统计”非常类似，所以我们完全可以借鉴此前的代码。
* */
import bean.ApacheLog;
import bean.PageCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.TreeSet;

/**
 * TODO 1、设置水位线60S;
 *      2、分组、开窗、聚合，并获取窗口结束时间；
 *      3、再按照窗口结束时间keyBy，将统一窗口的数据放到一起；
 *      4、设置两个状态（一个valuestate:用于存放窗口结束时间；
 *                     一个Liststate:用于存放数据；）
 *      5、设置定时器：窗口结束时间+10L;表示等窗口内的数据全部到达后开始计算（触发ontime()）；
 *      6、在ontime中进行排序操作，取topN；
 * */

public class Flink_Project_Page_TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建WatermarkStrategy
        WatermarkStrategy<ApacheLog> wms = WatermarkStrategy
                .<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                    @Override
                    public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                });

        env
                .readTextFile("input/apache.log")
                .map(line -> {
                    String[] data = line.split(" ");
                    SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    return new ApacheLog(data[0],
                            df.parse(data[3]).getTime(),
                            data[5],
                            data[6]);
                })
                .assignTimestampsAndWatermarks(wms)
                .keyBy(ApacheLog::getUrl)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<ApacheLog, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(ApacheLog value, Long accumulator) {
                        return accumulator + 1L;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                }, new ProcessWindowFunction<Long, PageCount, String, TimeWindow>() { // <url, count, endWindow>
                    @Override
                    public void process(String key,Context context, Iterable<Long> elements, Collector<PageCount> out) throws Exception {
                        out.collect(new PageCount(key,
                                elements.iterator().next(),
                                context.window().getEnd()));
                    }
                })
                .keyBy(PageCount::getWindowEnd)
                .process(new KeyedProcessFunction<Long, PageCount, String>() {

                    private ValueState<Long> timerTs;
                    private ListState<PageCount> pageState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        pageState = getRuntimeContext().getListState(new ListStateDescriptor<PageCount>("pageState", PageCount.class));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
                    }

                    @Override
                    public void processElement(PageCount value, Context ctx, Collector<String> out) throws Exception {
                        pageState.add(value);
                        if (timerTs.value() == null) {
                            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 10L);
                            timerTs.update(value.getWindowEnd());
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 换个排序的思路: 使用TreeSet的自动排序功能
                        TreeSet<PageCount> pageCounts = new TreeSet<>((o1, o2) -> {
                            if (o1.getCount() < o2.getCount()) return 1;
                                //  else if(o1.getCount() - o2.getCount() == 0) return 0;
                            else return -1;
                        });
                        for (PageCount pageCount : pageState.get()) {
                            pageCounts.add(pageCount);
                            if (pageCounts.size() > 3) { // 如果长度超过N, 则删除最后一个, 让长度始终保持N
                                pageCounts.pollLast();
                            }
                        }
                        StringBuilder sb = new StringBuilder();
                        sb.append("窗口结束时间: " + (timestamp - 10) + "\n");
                        sb.append("---------------------------------\n");
                        for (PageCount pageCount : pageCounts) {
                            sb.append(pageCount + "\n");
                        }
                        sb.append("---------------------------------\n\n");
                        out.collect(sb.toString());
                    }

                })
                .print();

        env.execute();
    }
}