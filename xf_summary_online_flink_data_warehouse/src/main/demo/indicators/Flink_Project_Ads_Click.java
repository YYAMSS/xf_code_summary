package indicators;

import bean.AdsClickLog;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.api.common.typeinfo.Types.TUPLE;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 1:36
 * @Version 1.0
 */
/*
* todo 各省份页面广告点击量实时统计
*
* */
/**
 * TODO 1、按照省份、广告ID 分组；
 *      2、对分组后的数据进行求和；
 * */
public class Flink_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.readTextFile("input/AdClickLog.csv")
                .map(line -> {
                    String[] datas = line.split(",");
                    return new AdsClickLog(
                            Long.valueOf(datas[0]),
                            Long.valueOf(datas[1]),
                            datas[2],
                            datas[3],
                            Long.valueOf(datas[4]));
                })
                .map(log -> Tuple2.of(Tuple2.of(log.getProvince(), log.getAdId()), 1L))
                //.returns(TUPLE(TUPLE(STRING, LONG), LONG))
                .keyBy((KeySelector<Tuple2<Tuple2<String, Long>, Long>, Tuple2<String, Long>>) value -> value.f0)
                .sum(1)
                .print("省份-广告");

        env.execute();
    }
}
