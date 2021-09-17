package functions;

import akka.io.Tcp;
import bean.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author xiefeng
 * @DATA 2021/9/17 1:30
 * @Version 1.0
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String, JSONObject> {

    private Connection connection = null;

    private OutputTag<JSONObject> objectOutputTag;

    private MapStateDescriptor<String,TableProcess> mapStateDescriptor;

    public TableProcessFunction() {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181");
    }

    public TableProcessFunction(OutputTag<JSONObject> hbaseOutPutTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.objectOutputTag = hbaseOutPutTag;
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject data = jsonObject.getJSONObject("data");
        TableProcess tableProcess = JSON.parseObject(data.toJSONString(), TableProcess.class);

        if(tableProcess != null){
            if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                checkTable(
                        tableProcess.getSinkTable(),
                        tableProcess.getSinkColumns(),
                        tableProcess.getSinkPk(),
                        tableProcess.getSinkExtend()
                );
            }
            //将数据写入广播状态中
            BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            String key = tableProcess.getSourceTable() + ":" + tableProcess.getOperateType();
            broadcastState.put(key,tableProcess);
        }
    }
    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //获取数据状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        System.out.println("value======" + value.toString());
        String key = value.getString("table") + ":" + value.getString("type");
        TableProcess tableProcess = broadcastState.get(key);



        if(tableProcess != null){
            JSONObject data = value.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            filterColumn(data,sinkColumns);

            String sinkType = tableProcess.getSinkType();
            value.put("sinkTable", tableProcess.getSinkTable());

            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                ctx.output(objectOutputTag,value);
            }else if(TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                out.collect(value);
            }
        }else{
            System.out.println("配置信息中不存在Keys: " + key);
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        entries.removeIf(next -> !columnList.contains(next.getKey()));

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if(sinkPk == null || sinkPk.equals("")){
            sinkPk = "id";
        }

        if(sinkExtend == null){
            sinkExtend = "";
        }

        //创建建表SQL
        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(GlobalContext.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");
        //将建表字段拆开
        String[] columns = sinkColumns.split(",");
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];

            if(sinkPk.equals(column)){
                createTableSQL.append(column).append(" varchar ").append(" primary key ");
            }else{
                createTableSQL.append(column).append(" varchar ");
            }

            if(i < columns.length-1){
                createTableSQL.append(",");
            }
        }

        createTableSQL.append(")").append(sinkExtend);

        String sql = createTableSQL.toString();
        System.out.println(sql);

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Phoenix 建表失败！");
        } finally {
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
