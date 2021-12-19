package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;

//@FunctionHint 指定UDTF函数处理之后 形成几列，每列代表什么
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeyWordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}
