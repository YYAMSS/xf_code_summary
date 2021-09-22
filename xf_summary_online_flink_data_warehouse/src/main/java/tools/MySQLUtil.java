package tools;

import bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author xiefeng
 * @DATA 2021/9/15 13:32
 * @Version 1.0
 */
public class MySQLUtil {

    public static <T> List<T> queryList(String sql, Class<T> cls, boolean underScoreToCamel)  {

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            //连接Mysql
            //Class.forName(GlobalContext.MYSQL_DRIVER);
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://192.168.40.102:3306/xxff?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");
            //编译sql，并给占位符赋值
            preparedStatement = connection.prepareStatement(sql);
            //执行查询
            resultSet = preparedStatement.executeQuery();

            //解析查询结果------
            ArrayList<T> list = new ArrayList<>();
            //取出元数据
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            //取出mysql中查询出来的数据，封装成javabean
            while (resultSet.next()){
                //封装javabean并加入集合
                T t = cls.newInstance();
                for (int i = 1; i <= columnCount; i++) {
                    //获取列名
                    String columnName = metaData.getColumnName(i);
                    if(underScoreToCamel){
                        columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //获取值
                    Object object = resultSet.getObject(i);

                    //给javaBean对象赋值
                    BeanUtils.setProperty(t,columnName,object);
                }
                list.add(t);
            }
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("配置信息失败！");
        } finally {
            //释放资源
            if(resultSet != null){
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process", TableProcess.class, true);
        System.out.println(tableProcesses);
    }
}
