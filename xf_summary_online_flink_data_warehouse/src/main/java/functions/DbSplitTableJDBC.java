package functions;

import bean.TableProcess;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tools.MySQLUtil;

import java.lang.ref.Reference;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author xiefeng
 * @DATA 2021/9/15 13:22
 * @Version 1.0
 */
public class DbSplitTableJDBC extends ProcessFunction<JSONObject,JSONObject> {
    //定义属性
    private OutputTag<JSONObject> OutputTag;

    //定义配置信息的Map
    private HashMap<String, TableProcess> tableProcessHashMap;

    //定义set用于记录当前phoenix中已经存在的表
    private HashSet<String> existTables;

    private Connection connection = null;


    public DbSplitTableJDBC(OutputTag<JSONObject> hbaseTag) {
        this.OutputTag = hbaseTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化配置信息的map,主要目的：存放从mysql中读取的单个配置表信息
        tableProcessHashMap = new HashMap<>();

        //初始化Phoenix已经存在表的set
        existTables = new HashSet<>();

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection("jdbc:phoenix:192.168.40.102:2181");

        refreshMeta();
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        },5000L,5000L);
    }

    private void refreshMeta() {
        System.out.println("开始读取Mysql配置信息！");
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            //获取源表信息
            String sourceTable = tableProcess.getSourceTable();
            //获取操作信息
            String operateType = tableProcess.getOperateType();

            String key = sourceTable + ":" + operateType;
            tableProcessHashMap.put(key,tableProcess);

            if(TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){

                boolean notExist = existTables.add(tableProcess.getSinkTable());

                if(notExist){
                    createPhoenixTable(
                            tableProcess.getSinkTable(),
                            tableProcess.getSinkColumns(),
                            tableProcess.getSinkPk(),
                            tableProcess.getSinkExtend()
                    );
                }
            }
        }
    }

    private void createPhoenixTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        if(sinkPk == null){
            sinkPk = "id";
        }
        if(sinkExtend == null){
            sinkExtend = "";
        }

        StringBuilder createSql = new StringBuilder("create table if not exists ").append(sinkTable).append("(");
        String[] fields = sinkColumns.split(",");

        for (int i = 0; i < fields.length; i++) {
            String field = fields[i];
            if(sinkPk.equals(field)){
                createSql.append(field).append(" varchar primary key ");
            }else{
                createSql.append(field).append(" varchar ");
            }

            if(i < fields.length -1){
                createSql.append(",");
            }
        }
        createSql.append(")");
        createSql.append(sinkExtend);
        System.out.println(createSql);

        PreparedStatement preparedStatement = null;

        try {
            preparedStatement = connection.prepareStatement(createSql.toString());
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("创建phoenix表" + sinkTable + "失败！");
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

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

        String table = value.getString("table");
        String type = value.getString("type");

        String key = table + ":" + type;

        TableProcess tableProcess = tableProcessHashMap.get(key);

        if(tableProcess != null){
            value.put("sink_table",tableProcess.getSinkTable());
            filterColumn(value.getJSONObject("data"),tableProcess.getSinkColumns());

            //判断数据是写入kafka还是Hbase
            if(TableProcess.SINK_TYPE_KAFKA.equals(tableProcess.getSinkType())){
                out.collect(value);
            }else if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
                ctx.output(OutputTag,value);
            }
        }
    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] split = sinkColumns.split(",");

        List<String> fieldList = Arrays.asList(split);

        Set<Map.Entry<String, Object>> entries = data.entrySet();
        //        while (iterator.hasNext()) {
        //            Map.Entry<String, Object> next = iterator.next();
        //                    if (!columnList.contains(next.getKey())) {
        //                        iterator.remove();
        //                   }
        //                }
        entries.removeIf(next -> !fieldList.contains(next.getKey()));
    }
}
