package com.kuaishou.data.udf.video;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.Kconfs;

/**
 * @author <zhouwenjia@kuaishou.com>
 * Created on 2022-01-11
 */
public class VersionFilter extends UDF {
    private static final Kconf<Map<String, String>> versionFilter =
            Kconfs.ofStringMap("videoData.whiteList.version_filter", new HashMap<>()).build();
    public boolean evaluate(long uid) {
        Map<String, String> map =
                versionFilter.get();
        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            if(uid>=Long.parseLong(s) && uid<Long.parseLong(map.get(s))) return true;
        }
        return false;
    }
}
