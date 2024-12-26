package org.hadoop.hive.learn.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 只展示第一个字符的操作
 */
public class SensitiveFunc extends UDF {
    public String evaluate(String str) {
        if (str == null || str.length() <= 1) {
            return str;
        }

        int length = str.length();
        String res = str.substring(0,1);

        StringBuilder sb = new StringBuilder(res);
        for (int i = 1; i < length; i++) {
            sb.append("*");
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        SensitiveFunc sensitiveFunc = new SensitiveFunc();
        String res = sensitiveFunc.evaluate("你好劜");
        System.out.println(res);
    }
}
