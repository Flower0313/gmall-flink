package com.atguigu.gmall.realtime.app.udf;

import com.atguigu.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @ClassName gmall-flink-KeyWordUDTF
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月23日16:22 - 周四
 * @Describe 关键词UDTF函数, 可以参考官网的写法:
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/table/functions/udfs.html#table-functions
 */

//输出去的参数名就叫word并且是String类型,可以声明ROW<s STRING,i INT>,然后输出Row.of("s",313)
@FunctionHint(output = @DataTypeHint("ROW<word String>"))
public class KeyWordUDTF extends TableFunction<Row> {
    /**
     * @param word 传入需要切割的词(String类型)
     * @target 切词, 这里会调用我们的切词工具
     */
    public void eval(String word) {
        try {
            System.out.println("word的格式:" + word);
            List<String> keyWordList = KeyWordUtil.splitKeyWord(word);
            for (String keyword : keyWordList) {
                //遍历写出即可
                collect(Row.of(keyword));
            }
        } catch (Exception e) {
            //若切词出现异常就返回原词
            collect(Row.of(word));
            e.printStackTrace();
        }
    }
}
