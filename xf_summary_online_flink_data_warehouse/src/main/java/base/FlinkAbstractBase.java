package base;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tools.PropertiesLoader;


/**
 * @Author xiefeng
 * @DATA 2021/9/11 15:19
 * @Version 1.0
 */
public abstract class FlinkAbstractBase {

    public StreamExecutionEnvironment env;
    public StreamTableEnvironment tabEnv;
    private static final String PROPERTIES_FILE_NAME = "application.properties";
    protected transient JSONObject config = null;



    public void apply() throws Exception {
        config = PropertiesLoader.load(PROPERTIES_FILE_NAME);
        init();
        transformation();
        execute();
    }

    protected void init() throws Exception {
        System.out.println("Flink application initial start!");

        System.setProperty("HADOOP_USER_NAME", "atguigu");
        FlinkStreamEnvironmentInitializer initializer = new FlinkStreamEnvironmentInitializer();
        this.env = initializer.initAndGetFlinkEnv(config);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        this.tabEnv = StreamTableEnvironment.create(env,settings);
    }

    protected abstract void transformation() throws Exception;

    protected void execute() throws Exception {
        System.out.println("Flink application execute start!");
        this.env.execute(config.getString("job_name"));
        System.out.println("Flink application execute start! job name:" + config.getString("job_name"));
    }


}
