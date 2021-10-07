package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 1:27
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;
}
