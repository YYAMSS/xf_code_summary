package operator.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:15
 * @Version 1.0
 */
public class Flink_Source_File {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> input = env.readTextFile("input");

        input.print("input>>>");

        env.execute();

    }

}
