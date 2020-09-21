package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.cdn.api.safety.CdnAuthSigner;


public class AcfunJudgePkeyValid {

    public boolean evaluate(String url, String domain, Long timestamp) {

        if (url == null || url.length() == 0 || domain == null || domain.length() == 0 || timestamp == null) {
            //            throw new UDFArgumentException("must take three arguments");
            System.out.println("url error");
        }

        return CdnAuthSigner
                .checkAuthInfoForAcFun(url, domain, timestamp);
    }

    public static void main(String[] args) {
        String url = "http://tx-safety-video.acfun.cn/mediacloud/acfun/acfun_video/hls/jq8AKoVSvVNc6p6bQWtgIG54eDACwGG_uiaKAfW30zFSYBUphnEveljUr4FOHHGY.00248.ts?pkey=AAPmou42VxJ59GhwgVE6dbd0mSRk0r3kkTx9XlQkKAQVlSZvYJto52koXTOghWHqamVkZLp2QBdeltsehPPgrT-fwEKIHyZGhIiqa9T59M2Y1AKLX6IO7rOF78fGeY88s-VEdI7bSpkNFRdThbljWUHX3sXvsOUUn4CVqwq84Ka7UdcKjz3ZpcG-qMAP5hIMTdWwLPCeBtmsULaYNONpEP_m-1S3dOBODJW08qisxHkJ4_53ah_l7VmXQxS8XN7-4zMuVio5xHfePz3GpgrEGV5w3KdbgHXYjTqAr2jHt2uMjojVowNMpLr4ckg5M00wAR873JtwAh8Y1eGN5UYdQYmc";
        String domain = "tx-safety-video.acfun.cn";
        AcfunJudgePkeyValid judgePkeyValid = new AcfunJudgePkeyValid();
        System.out.println("flag: " + judgePkeyValid.evaluate(url, domain, 1600510488586L));
    }
}