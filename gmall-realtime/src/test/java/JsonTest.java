import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName gmall-flink-JsonTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日20:16 - 周一
 * @Describe
 */
public class JsonTest {
    public static void main(String[] args) {
        String json = "{\"common\":{\"ar\":\"440000\",\"uid\":\"23\",\"os\":\"iOS 13.3.1\",\"ch\":\"Appstore\",\"is_new\":\"1\",\"md\":\"iPhone 8\",\"mid\":\"mid_13\",\"vc\":\"v2.1.134\",\"ba\":\"iPhone\"},\"page\":{\"page_id\":\"trade\",\"item\":\"1\",\"during_time\":18744,\"item_type\":\"sku_ids\",\"last_page_id\":\"cart\"},\"actions\":[{\"action_id\":\"trade_add_address\",\"ts\":1608292488372}],\"ts\":1608292479000}";

        JSONObject jsonObject = JSONObject.parseObject(json);
        System.out.println(jsonObject.getJSONObject("common").getString("is_new"));

    }
}
