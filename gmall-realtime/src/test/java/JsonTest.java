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
        String json = "{\"database\":\"gmall_flink\",\"before\":{\"tm_name\":\"臭奈儿\",\"logo_url\":\"flower\",\"id\":11},\"after\":{\"tm_name\":\"牛逼哄哄\",\"logo_url\":\"/static/defailt.jpg\",\"id\":13},\"type\":\"delete\",\"table\":\"base_trademark\"}";

        JSONObject jsonObject = JSONObject.parseObject(json);
        String table = jsonObject.getString("table");
        JSONObject after = jsonObject.getJSONObject("after");

        Set<String> strings = after.keySet();
        Collection<Object> values = after.values();


        StringBuilder head = new StringBuilder("upsert into ")
                .append(HBASE_SCHEMA)
                .append(".")
                .append("\"" + table + "\"").append("(")
                .append(StringUtils.join(strings, ","))
                .append(") ")
                .append("values('")
                .append(StringUtils.join(values, "','"))
                .append("')");
        System.out.println(head);


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
