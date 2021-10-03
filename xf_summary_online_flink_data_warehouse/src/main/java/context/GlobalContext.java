package context;

/**
 * @Author xiefeng
 * @DATA 2021/9/12 15:51
 * @Version 1.0
 */
public class GlobalContext {

    //kafka相关
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String GROUP_ID = "group.id";
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static final String KAFKA_GROUP_ID = "kafka_group_id";
    public static final String KAFKA_TOPIC = "kafka_topic";
    public static final String KAFKA_OFFSET = "kafka_offset";
    public static final String KAFKA_DEFAULT_TOPIC = "kafka_default_topic";
    public static final String KAFKA_TOPIC_ODS = "kafka_topic_ods";
    public static final String DWD_ORDER_INFO = "dwd_order_info";
    public static final String DWD_ORDER_DETAIL = "dwd_order_detail";
    public static final String DWM_ORDER_WIDE = "dwm_order_wide";
    public static final String DWD_PAYMENT_INFO = "dwd_payment_info";
    public static final String DWM_PAYMENT_WIDE = "dwm_payment_wide";
    public static final String ODS_BASE_LOG = "ods_base_log";
    public static final String DWD_PAGE_LOG = "dwd_page_log";
    public static final String DWD_START_LOG = "dwd_start_log";
    public static final String DWD_DISPLAY_LOG = "dwd_display_log";
    public static final String DWD_DIRTY_LOG = "dwd_dirty_log";
    public static final String DWM_UNIQUE_VISIT = "dwm_unique_visit";
    public static final String DWM_USER_JUMP_DETAIL = "dwm_user_jump_detail";



    //Mysql相关
    public static final String MYSQL_DBURL = "jdbc:mysql://192.168.40.102:3306/test?characterEncoding=utf-8&useSSL=false";
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_USER = "root";
    public static final String MYSQL_PWD = "123456";

    //HBase相关
    //Phoenix库名
    public static final String HBASE_SCHEMA = "XXFF_TEST_2021";
    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";


}
