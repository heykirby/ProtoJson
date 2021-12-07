package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.data.udf.video.utils.VideoStringListConfig;

public class IsKwaiCdnUDF extends UDF {

    public boolean evaluate(String input) {
        if (input == null || input.equals("")) {
            return false;
        }
        return VideoStringListConfig.kwaiCdnIPList.get().contains(input);
    }

    public static void main(String[] args) {
        IsKwaiCdnUDF isKwaiCdn = new IsKwaiCdnUDF();
        boolean result = isKwaiCdn.evaluate("120.221.219.75");
        System.out.println(result);
    }
}
