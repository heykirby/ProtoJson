package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.framework.util.UuidUtils;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-06-08
 */
public class EncryptValue extends UDF {
    public static String evaluate(long id) {
        return UuidUtils.getUuid(id);
    }
}
