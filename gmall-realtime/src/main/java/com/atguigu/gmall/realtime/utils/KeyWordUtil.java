package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName gmall-flink-KeyWordUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月23日15:03 - 周四
 * @Describe 分词工具类
 */
public class KeyWordUtil {
    /**
     * @param words 传入需要切割的字符
     * @return 返回切割好词的List集合
     */
    public static List<String> splitKeyWord(String words) {
        ArrayList<String> resultList = new ArrayList<>();

        /*
         * Explain
         * 参数2为是否使用智能分词
         * true：使用智能分词
         * false：使用最细粒度分词
         * */
        IKSegmenter ik = new IKSegmenter(new StringReader(words), false);
        Lexeme lex = null;
        List<String> keywordList = new ArrayList<String>();
        while (true) {
            try {
                if ((lex = ik.next()) != null) {
                    String lexemeText = lex.getLexemeText();
                    keywordList.add(lexemeText);
                } else {
                    //若切分完毕就退出
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return keywordList;
    }

    public static void main(String[] args) {
        splitKeyWord("尚硅谷");
    }
}
