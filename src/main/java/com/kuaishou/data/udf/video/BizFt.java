package com.kuaishou.data.udf.video;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.Kconfs;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-07-05
 */
public class BizFt extends UDF {
    private static final Kconf<Map<String, String>> ftConf =
            Kconfs.ofStringMap("videoData.CDN_BIZ_FT.ft", Collections.emptyMap()).build();
    public static String evaluate(String ft) {
        return ftConf.get().getOrDefault(ft, "未知部门");
    }
}
