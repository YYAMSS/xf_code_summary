package tools;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 4:57
 * @Version 1.0
 */
public class RedisUtil {

    public static JedisPool jedisPool = null;

    public static Jedis getJedis(){
        if(jedisPool == null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setMaxWaitMillis(2000);
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool = new JedisPool(jedisPoolConfig, "hadoop102", 6379);
            System.out.println("开辟连接池");
            return jedisPool.getResource();
        }else {
            return jedisPool.getResource();
        }
    }
}
