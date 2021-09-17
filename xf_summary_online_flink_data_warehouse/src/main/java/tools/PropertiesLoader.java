package tools;

import com.alibaba.fastjson.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author xiefeng
 * @DATA 2021/9/11 16:00
 * @Version 1.0
 */
public class PropertiesLoader {

    public static JSONObject load(String fileName) throws IOException {
        JSONObject jsonObject = new JSONObject();
        Properties p = new Properties();

        InputStream resourceAsStream = PropertiesLoader.class.getClassLoader().getResourceAsStream(fileName);//读取application.properties

        if(resourceAsStream == null){
            System.out.println("配置文件加载失败！");
        }
        p.load(resourceAsStream);
        for (Object key : p.keySet()) {
            jsonObject.put(key.toString(),p.getProperty(String.valueOf(key)));
        }
        return jsonObject;
    }
}
