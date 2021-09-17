package enums;

import com.twitter.chill.java.IterableRegistrar;

/**
 * @Author xiefeng
 * @DATA 2021/9/12 0:42
 * @Version 1.0
 */
public enum FlinkCheckPointEnum {
    AT_LEAST_ONCE("AT_LEAST_ONCE"),
    EXACTLY_ONCE("EXACTLY_ONCE");

    private  String checkPoint;

    FlinkCheckPointEnum(String checkpoint) {
        this.checkPoint = checkpoint;
    }
    public String getCheckPoint(){
        return checkPoint;
    }

    public static FlinkCheckPointEnum getByValue(String cp){
        for (FlinkCheckPointEnum value : values()) {
            if(value.getCheckPoint().equalsIgnoreCase(cp)){
                return value;
            }
        }
        return null;
    }
}
