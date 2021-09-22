package bean;

import java.math.BigDecimal;
import lombok.Data;

/**
 * @Author xiefeng
 * @DATA 2021/9/18 15:15
 * @Version 1.0
 */
@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;
}
