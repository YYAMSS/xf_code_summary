package functions;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import tools.ThreadPoolUtil;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 4:36
 * @Version 1.0
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements JoinDimFunction<T>{
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPoolExecutor = ThreadPoolUtil.getThreadPoolExecutor();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows
            @Override
            public void run() {
                //查询维度数据
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, getId(input));
                //补充维度信息
                join(input,dimInfo);
                //将数据写出
                resultFuture.complete(Collections.singleton(input));


            }
        });
    }


    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) {
        System.out.println("TimeOut:" + input);

    }
}
