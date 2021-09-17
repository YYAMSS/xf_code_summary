package test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author xiefeng
 * @DATA 2021/9/12 23:32
 * @Version 1.0
 */
public class ReadPropertiesTest {
    public static void main(String[] args) throws IOException {
        Properties p = new Properties();
        InputStream resourceAsStream = ReadPropertiesTest.class.getResourceAsStream("/application.properties");

        p.load(resourceAsStream);
        String chk_timeout = p.getProperty("chk_timeout");
        System.out.println(chk_timeout);


    }
}
