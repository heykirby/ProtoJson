package com.kuaishou.data.udf.video;

import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-11-04
 */
public class UdfTest extends UDF {
    HashMap<String, Long> map = new HashMap<>();
    public long evaluate(String str) {
        if (map.containsKey(str)) {
            map.put(str, map.get(str) + 1);
        } else {
            map.put(str, 1L);
        }
        return map.get(str);
    }
}
