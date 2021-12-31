package com.kuaishou.data.udf.video;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.kconf.client.Kconfs;


/**
 * @author yourname <zhouwenjia@kuaishou.com>
 * Created on 2021-12-30
 */
public class KMeetingCount extends UDF {
    public static void main(String[] args) {
        boolean evaluate = KMeetingCount.evaluate("185546807739416936");
        System.out.println(evaluate);
    }

    public static boolean evaluate(String id) {
        Set<String> set = Kconfs.ofStringSet("videoData.whiteList.kwai_meeting", new HashSet<>()).build().get();
        return set.contains(id);
    }
}
