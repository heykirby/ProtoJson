package com.kuaishou.data.udf.video;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconfs;

/**
 * @author yourname <zhouwenjia@kuaishou.com>
 * Created on 2022-01-11
 */
public class VersionFilter extends UDF {
    public boolean evaluate(long uid) {
        Map<String, String> map =
                Kconfs.ofStringMap("videoData.whiteList.version_filter", new HashMap<>()).build().get();
        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            if(uid>=Long.parseLong(s) && uid<Long.parseLong(map.get(s))) return true;
        }
        return false;
    }
}

//        List<Long> ans=new ArrayList<>();
//
//        map.entrySet().stream()
//                .sorted(Map.Entry.comparingByKey())
//                        .forEachOrdered(x->ans.add(Long.parseLong(x.getKey())));
//        for(int i=0;i<map.keySet().size();i++){
//            if(uid>=ans.get(i) && uid<Long.parseLong(map.get(String.valueOf(ans.get(i))))) return true;
//        }
//        return false;
