package com.kuaishou.data.udf.video;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.Kconfs;

public class ZhihuProductName2AppName extends UDF {
    private static final Kconf<Map<String, String>> appNameKconfMap = Kconfs.ofStringMap("videoData.map.ZhihuProductName2AppName",
            Collections.emptyMap()).build();
    public String evaluate(String productName) {
        if (productName == null || productName.equals("")) {
            return "must input productName";
        }
        return appNameKconfMap.get().getOrDefault(productName, "UNKNOWN");
    }
}
