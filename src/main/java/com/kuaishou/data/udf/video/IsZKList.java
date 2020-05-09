package com.kuaishou.data.udf.video;

import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDF;
import com.kuaishou.data.udf.video.utils.ZkUtils;

/**
 * @author xiazehui <xiazehui@kuaishou.com>
 * Created on 2020-04-27
 */
public class IsZKList extends UDF {

    private static Set zkValue;

    public Set evaluate(String path) throws Exception {
        if (path == null || path.equals("")) {
            return null;
        }
        if (zkValue == null) {
            zkValue = ZkUtils.getValue(path);
        }
        return zkValue;
    }

    public boolean evaluate(String path, String input) throws Exception {
        if (path == null || path.equals("") || input == null || input.equals("")) {
            return false;
        }
        return evaluate(path).contains(input);
    }

}
