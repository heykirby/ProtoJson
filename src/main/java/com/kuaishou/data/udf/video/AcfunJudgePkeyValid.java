package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.cdn.api.safety.CdnAuthSigner;


public class AcfunJudgePkeyValid extends UDF {

    public boolean evaluate(String url, String domain, Long timestamp) throws UDFArgumentException {

        if (url == null || url.length() == 0 || domain == null || domain.length() == 0 || timestamp == null) {
            throw new UDFArgumentException("must take three arguments");
        }

        return CdnAuthSigner
                .checkAuthInfoForAcFun(url, domain, timestamp);
    }
}
