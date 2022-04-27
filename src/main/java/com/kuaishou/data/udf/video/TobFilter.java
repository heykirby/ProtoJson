package com.kuaishou.data.udf.video;


import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.Kconfs;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-04-27
 */
public class TobFilter extends UDF {
    private static final Kconf<Set<String>> appIdWhiteSet = Kconfs.ofStringSet("videoLive.tobServer.realAppList", new HashSet<>()).build();
    public static boolean evaluate(String appId) {
       return appIdWhiteSet.get().contains(appId);
    }
}
