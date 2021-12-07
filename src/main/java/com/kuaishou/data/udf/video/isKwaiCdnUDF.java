package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import com.kuaishou.data.udf.video.utils.StringListConfigKey;

public class isKwaiCdnUDF extends UDF {

    public boolean evaluate(Text input) {
        if (input == null || input.equals("")) {
            return false;
        }
        return StringListConfigKey.kwaiCdnIPList.get().contains(input);
    }

    public boolean evaluate(String input) {
        if (input == null || input.equals("")) {
            return false;
        }
        return StringListConfigKey.kwaiCdnIPList.get().contains(input);
    }

    public static void main(String[] args) {

        isKwaiCdnUDF isKwaiCdn = new isKwaiCdnUDF();
        boolean result = isKwaiCdn.evaluate(new Text("120.221.219.75"));
        System.out.println(result);

        result = isKwaiCdn.evaluate(new Text("114.114.114.114"));
        System.out.println(result);
    }
}
