package com.kuaishou.data.udf.video;

import com.kuaishou.cdn.urlkeeper.client.UrlKeeperClient;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.net.URI;
import java.net.URISyntaxException;

public class DecryptUrl extends UDF {

    public String evaluate(String originUrl) {
        if (originUrl == null || originUrl.length() == 0) {
            return "";
        }

        try {
            URI url = new URI(originUrl);
            if (url.getPath().startsWith("/ksc")) {
                String realPath = UrlKeeperClient.getOriginalUrl(url.getPath());
                return originUrl.split("/ksc")[0] + realPath;
            }
        } catch (URISyntaxException ignored) {
        }

        return originUrl;
    }

    public static void main(String[] args) {
        String url = "http://59.81.8.149/ksc1/IPEIplXoEviF3SUQb7iST7xd7yi_kYeSbm2hacYvsWxttfUSJWJlJtlJGl32IuhYuDdA5Pc0s06CibINX6qCt6RlvzDfQim1_-SIhbSfQcx-mZtGuHpHKAy71FotExYdK1QgyfGCeQKRNjXgFeYrq0A44CJQfb-KsC-Hu-fkmEoqky2L-ewl6Hz-TFVBXoqY.mp4?tag=1-1592209379-h-0-6jwmpuhm98-d73ee3fdd4281b7d&tt=hd15&di=ddc44803&bp=10231";
        DecryptUrl decryptUrl = new DecryptUrl();
        System.out.println("flag: " + decryptUrl.evaluate(url));
    }
}
