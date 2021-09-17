package main;
import business.ods.FlinkCDCAPI;

/**
 * @Author xiefeng
 * @DATA 2021/9/11 15:14
 * @Version 1.0
 */
public class FlinkCDCEntryPointStream {
    public static void main(String[] args) {

        try {
            System.out.println("--------------Flink application start!---------");
            new FlinkCDCAPI().apply();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
