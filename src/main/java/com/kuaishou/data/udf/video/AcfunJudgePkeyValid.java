package com.kuaishou.data.udf.video;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.cdn.api.safety.CdnAuthSigner;


public class AcfunJudgePkeyValid extends UDF {

    public static String evaluate(String url, String domain, Long timestamp) throws UDFArgumentException {

        if (url == null || url.length() == 0 || domain == null || domain.length() == 0 || timestamp == null) {
            throw new UDFArgumentException("must take three arguments");
        }
        try {
            return String.valueOf(CdnAuthSigner.checkAuthInfoForAcFun(url, domain, timestamp));
        }catch (Exception e){
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            return sw.toString();
        }
    }
}
