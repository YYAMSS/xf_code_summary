package business.dws;

import base.FlinkAbstractBase;
import bean.ProvinceStats;
import context.GlobalContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import tools.ClickHouseUtil;
import tools.MyKafkaUtil;

/**
 * @Author xiefeng
 * @DATA 2021/10/4 13:24
 * @Version 1.0
 */
public class ProvinceStatsApp extends FlinkAbstractBase {
    @Override
    protected void transformation() throws Exception {

        //TODO 使用DDL的方式创建表,提取时间戳字段生成WaterMark
        this.tabEnv.executeSql(
                "create table order_wide( " +
                            "order_id bigint, " +
                            "province_id bigint, " +
                            "province_name string, " +
                            "province_area_code string, " +
                            "province_iso_code string, " +
                            "province_3166_2_code string, " +
                            "total_amount decimal, " +
                            "create_time string, " +
                            "rowtime as TO_TIMESTAMP(create_time), " +
                            "WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND " +
                            ") WITH(" +
                            MyKafkaUtil.getKafkaDDL(GlobalContext.DWM_ORDER_WIDE, GlobalContext.GROUP_ID) +
                            ")");

        //测试打印
        //tableEnv.executeSql("select * from order_wide").print();

        //TODO 执行查询 开窗、分组、聚合
        Table tableResult = this.tabEnv.sqlQuery(
                 "select  " +
                 "    DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt,  " +
                 "    DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt,  " +
                 "    province_id,  " +
                 "    province_name,  " +
                 "    province_area_code,  " +
                 "    province_iso_code,  " +
                 "    province_3166_2_code,  " +
                 "    sum(total_amount) order_amount,  " +
                 "    count(distinct order_id) order_count,  " +
                 "    UNIX_TIMESTAMP()*1000 ts  " +
                 "from order_wide  " +
                 "group by   " +
                 "    province_id,  " +
                 "    province_name,  " +
                 "    province_area_code,  " +
                 "    province_iso_code,  " +
                 "    province_3166_2_code,  " +
                 "    TUMBLE(rowtime, INTERVAL '10' SECOND)");

        //TODO 将查询结果的动态表转换为流
        DataStream<ProvinceStats> provinceStatsDataStream = this.tabEnv.toAppendStream(tableResult, ProvinceStats.class);
        provinceStatsDataStream.print(">>>>>>>>>>>>");

        //TODO 将数据写入ClickHouse
        provinceStatsDataStream.addSink(ClickHouseUtil.getSinkFunc("insert into province_stats_201109 values(?,?,?,?,?,?,?,?,?,?)"));

    }
}
