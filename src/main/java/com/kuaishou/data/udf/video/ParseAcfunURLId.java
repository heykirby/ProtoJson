package com.kuaishou.data.udf.video;

import java.net.URI;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.mediacloud.FileName;
import com.kuaishou.mediacloud.IdNameUriParser;

public class ParseAcfunURLId extends UDF {


    public String evaluate(String url) throws UDFArgumentException {

        if (url == null || url.length() == 0) {
            throw new UDFArgumentException("must take one arguments");
        }

        String res = null;
        //        try {
        //             res = AcFunUrlTrans.parseSubTaskId(url);
        //        } catch (Exception e) {
        //            e.printStackTrace();
        //        }

        try {
            URI uri = URI.create(url);
            FileName fileName = IdNameUriParser.parseUri(uri);
            res = String.valueOf(fileName.getTaskId());
        } catch (Exception e) {
        }

        return res;
    }
}
