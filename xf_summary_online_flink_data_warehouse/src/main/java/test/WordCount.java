package test;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author xiefeng
 * @DATA 2021/9/14 23:24
 * @Version 1.0
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setMaxParallelism(2);
        // 2. 读取文件
        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 9999);
        // 3. 转换数据格式
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = lineDSS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }

            }
        }).setParallelism(2);
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = stringSingleOutputStreamOperator
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return new Tuple2<String, Long>(value, 1L);
                    }
                }).setParallelism(2);


        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                .keyBy(t -> t.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                .sum(1).setParallelism(2);
        // 6. 打印
        result.print();
        // 7. 执行
        env.execute("WordCount");

    }

}
