package com.atguigu.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {
    public static List<String> analyze(String text) {
        StringReader reader = new StringReader(text);
        //true 是否开启智能分词器
        IKSegmenter ikSegmenter = new IKSegmenter(reader,false);
        List<String> keywordList = new ArrayList<>();
        Lexeme lexeme = null;
        try {
            while ((lexeme=ikSegmenter.next())!=null){
                keywordList.add(lexeme.getLexemeText());
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return keywordList;
    }

    public static void main(String[] args) throws IOException {
        List<String> analyze = analyze("小米10 8+128G 深空灰色 移动联通电信4G");
        System.out.println(analyze);
    }
}
