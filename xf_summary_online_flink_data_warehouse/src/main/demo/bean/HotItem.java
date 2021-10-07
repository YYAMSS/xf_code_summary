package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 2:07
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class HotItem {
    private Long itemId;
    private Long count;
    private Long windowEndTime;
}
