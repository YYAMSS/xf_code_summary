package main;
import business.dwd.DwdSplitJDBC;

/**
 * @Author xiefeng
 * @DATA 2021/9/11 15:14
 * @Version 1.0
 */
public class FlinkEntryPointStreamDWDJDBC {
    public static void main(String[] args) {

        try {
            System.out.println("--------------Flink application start!---------");
            new DwdSplitJDBC().apply();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


