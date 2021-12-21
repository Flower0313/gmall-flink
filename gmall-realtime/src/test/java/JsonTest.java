import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import org.apache.commons.lang.StringUtils;
import scala.math.Ordering;


import java.util.*;

import static com.atguigu.gmall.realtime.common.GmallConfig.HBASE_SCHEMA;

/**
 * @ClassName gmall-flink-JsonTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日20:16 - 周一
 * @Describe
 */
public class JsonTest {
    public static void main(String[] args) {
        String json = "{\"common\":{\"ar\":\"110000\",\"uid\":\"5\",\"os\":\"Android 10.0\",\"ch\":\"oppo\",\"is_new\":\"0\",\"md\":\"Huawei P30\",\"mid\":\"mid_18\",\"vc\":\"v2.1.134\",\"ba\":\"Huawei\"},\"page\":{\"page_id\":\"good_detail\",\"item\":\"9\",\"during_time\":13182,\"item_type\":\"sku_id\",\"last_page_id\":\"login\",\"source_type\":\"query\"},\"displays\":[{\"display_type\":\"query\",\"item\":\"10\",\"item_type\":\"sku_id\",\"pos_id\":2,\"order\":1},{\"display_type\":\"promotion\",\"item\":\"5\",\"item_type\":\"sku_id\",\"pos_id\":1,\"order\":2},{\"display_type\":\"query\",\"item\":\"2\",\"item_type\":\"sku_id\",\"pos_id\":3,\"order\":3},{\"display_type\":\"promotion\",\"item\":\"1\",\"item_type\":\"sku_id\",\"pos_id\":2,\"order\":4},{\"display_type\":\"query\",\"item\":\"5\",\"item_type\":\"sku_id\",\"pos_id\":1,\"order\":5},{\"display_type\":\"promotion\",\"item\":\"8\",\"item_type\":\"sku_id\",\"pos_id\":5,\"order\":6},{\"display_type\":\"promotion\",\"item\":\"6\",\"item_type\":\"sku_id\",\"pos_id\":5,\"order\":7},{\"display_type\":\"promotion\",\"item\":\"2\",\"item_type\":\"sku_id\",\"pos_id\":5,\"order\":8}],\"actions\":[{\"item\":\"1\",\"action_id\":\"get_coupon\",\"item_type\":\"coupon_id\",\"ts\":1640437031591}],\"ts\":1640437025000}";

        JSONObject jsonObject = JSON.parseObject(json);
        JSONArray displays = jsonObject.getJSONArray("displays");
        //取出单条数据
        for (Object display : displays) {
            System.out.println(display);
        }


    }

    public static void filter(JSONObject json, String str) {
        String[] split = str.split(",");
        List<String> list = Arrays.asList(split);

        Iterator<Map.Entry<String, Object>> iterator = json.entrySet().iterator();

        System.out.println("处理前");
        while (iterator.hasNext()) {
            if (!list.contains(iterator.next().getKey()))
                iterator.remove();
        }

        System.out.println(json.toString());


    }


}
