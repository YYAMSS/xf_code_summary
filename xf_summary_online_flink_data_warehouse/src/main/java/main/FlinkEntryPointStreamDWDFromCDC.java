package main;

import business.dwd.CashReserveSplitCDC;

/**
 * @Author xiefeng
 * @DATA 2021/9/17 0:54
 * @Version 1.0
 */
public class FlinkEntryPointStreamDWDFromCDC {
    public static void main(String[] args) {

        System.out.println("--------------Flink application start!---------");
        try {
            new CashReserveSplitCDC().apply();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
