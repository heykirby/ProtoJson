package com.kuaishou.data.udf.video;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

import com.kuaishou.cdn.api.safety.CdnAuthSigner;


public class AcfunJudgePkeyValid extends UDF {

    public String evaluate(String url, String domain, Long timestamp) throws UDFArgumentException {

        if (url == null || url.length() == 0 || domain == null || domain.length() == 0 || timestamp == null) {
            throw new UDFArgumentException("must take three arguments");
        }
        try {
            return CdnAuthSigner
                    .checkAuthInfoForAcFun(url, domain, timestamp);
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    public static void main(String[] args) throws UDFArgumentException {
        String url =
                "http://tx-safety-video.acfun.cn/mediacloud/acfun/acfun_video/hls/JquYMt2x"
                        + "-HLjJWAZsqK0jGYm5qVSDU2qB9cATx_9OBaV_xDeQCdB0O2tTFt8kfs3.00009"
                        + ".ts?pkey"
                        +
                        "=AAMuSHrHibH48r5XuqUVuKZ9BK_3Oc_9kADiEa0p9VZqG9VYkBY_55fJWQgcvz5FloNOk_RvT26EfzkJziblbwSijS4mFYcoBWDKPCzoSvfk-e8eiuFD5UD0GW9tQvu14x2Lu6B9qsR81R5P-nS1EptW9zqrtcAFcPkQm01k3wCde8lr7CSJcxL7XOK27C4lNyEG9jPu_VJl4JCJXPRdw71DsX28vhZA8yITwRJrEV__aspeFnWu8FgDG_A7Vs6X5Ep6PR4E9mIS0I5jh_Xq0mA4IVZWMZaWlnSuRa_DfiYKUbU2S5tlAby5sQDQd8TYDx6Xo1MGzMSY1hy31Vw9noqZ";
        String domain = "tx-safety-video.acfun.cn";
        AcfunJudgePkeyValid judgePkeyValid = new AcfunJudgePkeyValid();
        System.out.println("flag: " + judgePkeyValid.evaluate(url, domain, 1600759424440L));
    }
}