package base;

import com.alibaba.fastjson.JSONObject;
import enums.FlinkCheckPointEnum;
import enums.FlinkTimeEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;


/**
 * @Author xiefeng
 * @DATA 2021/9/11 16:34
 * @Version 1.0
 */
public class FlinkStreamEnvironmentInitializer {



    public StreamExecutionEnvironment initAndGetFlinkEnv(JSONObject config) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //-------------------------Flink获取配置信息-------------------------
        String chk_interval = config.getString("chk_interval");//checkpoint周期，毫秒值
        String chk_mode = config.getString("chk_mode");//checkpoint类型
        String chk_timeout = config.getString("chk_timeout");//checkpoint超时时间
        //String backend_path = config.getString("backend_path");//checkpoint backend备份路径
        String backend_path = "hdfs://hadoop102:8020/xf_checkpoint_test_202109/ck";
        String state_backend_type = config.getString("backend_type");//checkpoint backend备份类型
        String time_type = config.getString("time_type");//flink引擎处理时间类型

        //-------------------------Flink checkpoint设置-------------------------
        if(StringUtils.isNotBlank(chk_interval) && StringUtils.isNotBlank(chk_mode) && StringUtils.isNotBlank(chk_timeout)){

            //设置chk_interval
            env.enableCheckpointing(Long.parseLong(chk_interval));
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();

            //设置chk_mode
            FlinkCheckPointEnum flinkcheckpointEnum = FlinkCheckPointEnum.getByValue(chk_mode);
            if(flinkcheckpointEnum == FlinkCheckPointEnum.AT_LEAST_ONCE){
                checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
            }else if (flinkcheckpointEnum == FlinkCheckPointEnum.EXACTLY_ONCE) {
                checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            }

            //设置chk_timeout
            checkpointConfig.setCheckpointTimeout(Long.parseLong(chk_timeout));
            checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig
            .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        }
        //-------------------------Flink状态的后端存储-------------------------
        if(StringUtils.isNotBlank(backend_path)){
            StateBackend stateBackend = null;
            if("fs".equalsIgnoreCase(state_backend_type)){
                stateBackend = new FsStateBackend(backend_path);
            }else if ("rocksdb".equalsIgnoreCase(state_backend_type)){
                stateBackend = new RocksDBStateBackend(backend_path);
            }
            env.setStateBackend(stateBackend);
        }

        //-------------------------Flink时间设置-------------------------
        FlinkTimeEnum flinkTimeEnum = FlinkTimeEnum.getByValue(time_type);
        switch (flinkTimeEnum){
            case PROCESSING_TIME:
                env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                break;
            case EVENT_TIME:
                env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                break;
            case INGESTION_TIME:
                env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                break;
        }

        return env;
    }



}
