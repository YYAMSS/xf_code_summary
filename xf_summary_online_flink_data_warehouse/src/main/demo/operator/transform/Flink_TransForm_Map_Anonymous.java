package operator.transform;

/**
 * @Author xiefeng
 * @DATA 2021/10/6 22:39
 * @Version 1.0
 */
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_TransForm_Map_Anonymous {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 2, 3, 4, 5)
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value * value;
                    }
                })
                .print();

/*        //Lambda表达式表达式
         env.fromElements(1, 2, 3, 4, 5)
                .map(ele -> ele * ele)
                .print();*/



        env.execute();
    }
}
//静态内部类
/*
public class Flink01_TransForm_Map_StaticClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
                .fromElements(1, 2, 3, 4, 5)
                .map(new MyMapFunction())
                .print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Integer, Integer> {

        @Override
        public Integer map(Integer value) throws Exception {
            return value * value;
        }
    }

}*/
