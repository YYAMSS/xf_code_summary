package tools;

import scala.reflect.internal.Trees;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @Author xiefeng
 * @DATA 2021/9/22 4:40
 * @Version 1.0
 */
public class ThreadPoolUtil {
    //私有方法，声明线程池
    private static ThreadPoolExecutor threadPoolExecutor;
    //私有化构造方法
    private ThreadPoolUtil(){
    }
    public static ThreadPoolExecutor getThreadPoolExecutor(){

        if(threadPoolExecutor == null){
            synchronized (ThreadPoolUtil.class){
                if(threadPoolExecutor == null){
                    threadPoolExecutor = new ThreadPoolExecutor(4,
                            20,
                            600L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
