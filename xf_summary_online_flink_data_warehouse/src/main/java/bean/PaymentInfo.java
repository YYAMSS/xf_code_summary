package bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @Author xiefeng
 * @DATA 2021/9/23 0:14
 * @Version 1.0
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;


}
