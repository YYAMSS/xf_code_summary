package functions;

import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * @Author xiefeng
 * @DATA 2021/9/16 23:16
 * @Version 1.0
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    private Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection("jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181");
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        //获取keys 和 values
        JSONObject data = value.getJSONObject("data");
        Set<String> keys = data.keySet();
        Collection<Object> values = data.values();

        System.out.println("value+++++=======" + value.toString());

        //获取表名
        String tableName = value.getString("sinkTable");

        //创建插入数据的sql
        String upsertSql = genUpsertSql(tableName, keys, values);
        System.out.println("phoenix的sql语句" + upsertSql );

        preparedStatement = connection.prepareStatement(upsertSql);

        preparedStatement.executeUpdate();
        connection.commit();

        //判断是否为更新操作，如果是，则删除redis中的数据
        //  ------------   接下来在写----------

    }

    private String genUpsertSql(String tableName, Set<String> keys, Collection<Object> values) {
        return "upsert into " + GlobalContext.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(keys,",")+ ")"
                +"values('" + StringUtils.join(values,"','") + "')";
    }
}
