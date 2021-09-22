package tools;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import context.GlobalContext;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 5:21
 * @Version 1.0
 */
public class PhoenixUtil {
    private static Connection connection = null;

    public static Connection getConnection(){
        try {
            Class.forName(GlobalContext.PHOENIX_DRIVER);
            return DriverManager.getConnection(GlobalContext.PHOENIX_SERVER);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败!");
        }
    }


    public static <T> List<T> queryList(String sql, Class<T> cls, boolean underScoreToCamel) {

        ArrayList<T> list = new ArrayList<>();
        PreparedStatement preparedStatement = null;

        try {
            if (connection == null) {
                connection = getConnection();
            }

            preparedStatement = connection.prepareStatement(sql);

            //执行查询
            ResultSet resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()){
                T t = cls.newInstance();

                for (int i = 0; i < columnCount + 1; i++) {
                    String columnName = metaData.getColumnName(i);
                    if(underScoreToCamel){
                        columnName = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL).convert(columnName);
                    }
                    //取出数据
                    Object value = resultSet.getObject(i);
                    BeanUtils.setProperty(t,columnName,value);
                }
                list.add(t);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

        }
        return list;
    }
}
