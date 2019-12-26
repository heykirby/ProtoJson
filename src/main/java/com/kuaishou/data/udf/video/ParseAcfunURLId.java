package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.data.udf.video.utils.AcFunUrlTrans;

public class ParseAcfunURLId extends UDF {


    public String evaluate(String url) throws UDFArgumentException {

        if (url == null || url.length() == 0) {
            throw new UDFArgumentException("must take one arguments");
        }

        String res=null;
        try {
             res = AcFunUrlTrans.parseSubTaskId(url);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return res;
    }
}
