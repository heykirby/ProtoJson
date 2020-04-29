package com.kuaishou.data.udf.video;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.common.collect.Lists;
import com.kuaishou.data.udf.video.utils.StringListConfigKey;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2020-04-27
 */
public class GetOrInZKList extends UDF {


    public Set evaluate(String path) {
        if (path == null || path.equals("")) {
            return null;
        }


        switch (path) {
            case "appVersionTopList":
                return StringListConfigKey.appVersionTopList.appVersionTopList.get();
            case "kwaiCdnIPList":
                return StringListConfigKey.appVersionTopList.kwaiCdnIPList.get();
            case "esLogWhiteList":
                return StringListConfigKey.esLogWhiteList.kwaiCdnIPList.get();
            case "esLogWhiteListDeviceId":
                return StringListConfigKey.esLogWhiteListDeviceId.kwaiCdnIPList.get();
            case "esLogWhiteListAppVersion":
                return StringListConfigKey.esLogWhiteListAppVersion.kwaiCdnIPList.get();
            default:
                return null;
        }
    }

    public boolean evaluate(String path, String input) {
        if (path == null || path.equals("") || input == null || input.equals("")) {
            return false;
        }
        return evaluate(path).contains(input);
    }

}
