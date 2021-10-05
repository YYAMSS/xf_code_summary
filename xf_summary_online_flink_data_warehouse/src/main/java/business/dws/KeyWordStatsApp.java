package business.dws;

import base.FlinkAbstractBase;
import bean.KeywordStats;
import bean.KeywordTableFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import tools.ClickHouseUtil;
import tools.MyKafkaUtil;

/**
 * @Author xiefeng
 * @DATA 2021/10/4 13:25
 * @Version 1.0
 */
public class KeyWordStatsApp extends FlinkAbstractBase {


    @Override
    protected void transformation() throws Exception {


        //TODO 2.使用DDL方式读取Kafka数据创建表
        String topic = "dwd_page_log";
        String groupId = "keyword_stats_app_201109";

        this.tabEnv.executeSql("create table page_view( " +
                "common Map<String,String>, " +
                "page Map<String,String>, " +
                "ts bigint, " +
                "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND" +
                ")with(" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //打印测试
//        tableEnv.executeSql("select * from page_view")
//                .print();

        //TODO 3.过滤数据
        Table fullWordTable = this.tabEnv.sqlQuery("" +
                "select " +
                "    page['item'] fullWord, " +
                "    rowtime " +
                "from page_view " +
                "where page['item_type'] = 'keyword' " +
                "and page['last_page_id'] = 'search' " +
                "and page['item'] is not null");

        //打印测试
        //tableEnv.toAppendStream(fullWordTable, Row.class).print();

        //TODO 4.注册UDTF函数并进行分词处理
        this.tabEnv.createTemporarySystemFunction("SplitFunction", KeywordTableFunction.class);
        Table splitWordTable = this.tabEnv.sqlQuery("SELECT word, rowtime FROM " + fullWordTable + ", LATERAL TABLE(SplitFunction(fullWord))");

        //打印测试
        //tableEnv.toAppendStream(splitWordTable, Row.class).print();

        //TODO 5.开窗、分组、聚合
        Table resultTable = this.tabEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from  " + splitWordTable +
                " group by word, " +
                "  TUMBLE(rowtime, INTERVAL '10' SECOND)");

        //TODO 6.转换为流并写入ClickHouse
        DataStream<KeywordStats> keywordStatsDataStream = this.tabEnv.toAppendStream(resultTable, KeywordStats.class);
        keywordStatsDataStream.print();
        keywordStatsDataStream.addSink(ClickHouseUtil.getSinkFunc("insert into keyword_stats_201109(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));


    }
}
