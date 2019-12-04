package com.kuaishou.data.udf.video;

import java.net.URI;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.google.protobuf.InvalidProtocolBufferException;
import com.kuaishou.data.udf.video.utils.AcFunUrlTrans;

public class ParseAcfunURL extends UDF {


    public String evaluate(String url) throws UDFArgumentException {

        if (url == null || url.length() == 0) {
            throw new UDFArgumentException("must take one arguments");
        }

        String res=null;
        try {
             res = AcFunUrlTrans.parseSubTask(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
}
