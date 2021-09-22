package functions;

import com.alibaba.fastjson.JSONObject;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 4:53
 * @Version 1.0
 */
public interface JoinDimFunction<T> {

    //根据数据获取对应的维度ID
    String getId(T input);

    //将维度信息补充到实时数据上
    void join(T input, JSONObject dimInfo);
}
