package com.kuaishou.data.udf.video;

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
    public boolean evaluate(int id) {
        Map<String, String> map =
                Kconfs.ofStringMap("videoData.whiteList.version_filter", new HashMap<>()).build().get();
        List<Integer> ans=new ArrayList<>();
        map.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                        .forEachOrdered(x->ans.add(Integer.parseInt(x.getKey())));
        for(int i=0;i<ans.size();i++){
            if(id>=ans.get(i)&&id<Integer.parseInt(map.get(String.valueOf(ans.get(i))))) return true;
        }
        return false;
    }
}
