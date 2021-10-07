package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 2:15
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PageCount {
    private String url;
    private Long count;
    private Long windowEnd;
}
