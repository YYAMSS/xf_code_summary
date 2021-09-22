package functions;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import context.GlobalContext;
import redis.clients.jedis.Jedis;
import tools.PhoenixUtil;
import tools.RedisUtil;

import java.util.List;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 4:50
 * @Version 1.0
 */
public class DimUtil {


    public static JSONObject getDimInfo(String tableName, String value) {

        //先查询Redis数据
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = tableName + ":" + value;

        String jsonStr = jedis.get(redisKey);
        if(jsonStr != null){
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            //对于指定的key设置失效时间
            jedis.expire(redisKey,24*60*60);
            jedis.close();
            //返回结果
            return jsonObject;
        }

        //当在代码中查不到该数据时，需要在Phoenix中查询；
        //封装SQL语句
        String querySQL = "select * from " + GlobalContext.HBASE_SCHEMA + "." + tableName + "where id='" + value + "'";

        List<JSONObject> queryList = PhoenixUtil.queryList(querySQL, JSONObject.class, false);
        JSONObject jsonObject = queryList.get(0);

        jedis.set(redisKey,jsonObject.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();
        return jsonObject;
    }

    public static void delRedisDim(String tableName,String value){
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = tableName + ":" + value;
        System.out.println(redisKey);

        //执行删除操作
        jedis.del(redisKey);
        jedis.close();
    }
}
