package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author xiefeng
 * @DATA 2021/10/7 2:21
 * @Version 1.0
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ApacheLog {
    private String Id;
    private Long EventTime;
    private String type;
    private String Url;
}