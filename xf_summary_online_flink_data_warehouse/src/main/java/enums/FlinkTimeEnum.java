package enums;

import scala.reflect.internal.Trees;

/**
 * @Author xiefeng
 * @DATA 2021/9/12 1:13
 * @Version 1.0
 */
public enum FlinkTimeEnum {

    PROCESSING_TIME("processingTime"),
    EVENT_TIME("eventTime"),
    INGESTION_TIME("ingestion_time");

    private  String timetype;

    FlinkTimeEnum(String timeType){this.timetype = timeType;}

    public String getTimetype(){
        return timetype;
    }

    public static FlinkTimeEnum getByValue(String ts){
        for (FlinkTimeEnum value : values()) {
            if(value.getTimetype().equalsIgnoreCase(ts)){
                return value;
            }
        }
        return null;
    }

}
