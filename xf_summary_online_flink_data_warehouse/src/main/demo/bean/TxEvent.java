package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 1:40
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;
}
