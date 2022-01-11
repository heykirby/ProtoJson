package com.kuaishou.data.udf.video;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconfs;

/**
 * @author yourname <zhouwenjia@kuaishou.com>
 * Created on 2022-01-11
 */
public class platformFilter extends UDF {
    public boolean evaluate(String platform) {
        Set<String> set = Kconfs.ofStringSet("videoData.whiteList.platformFilter", new HashSet<>()).build().get();
        String s = platform.toUpperCase(Locale.ROOT);
        return set.contains(s);
    }
}
