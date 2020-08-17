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
                String suffix = "";
                if (!originUrl.endsWith(url.getPath()))
                    suffix = originUrl.split(url.getPath())[1];
                return originUrl.split("/ksc")[0] + realPath + suffix;
            }
        } catch (URISyntaxException | IllegalArgumentException ignored) {
        }

        return originUrl;
    }

    public static void main(String[] args) {
        String url = "http://123.6.12.228/ksc1/TXDQIYF10QIv-kKQ1ryl1ICGLNBjGfQJ8OWlUrhGn-GnVnJgFnGhyNw54-tyEeyGwywuZD71YLUwHRd2ua7lagl9iDv0imyWR*0bxA_MngHh5HE-xB_cFI1yWJjFyw2WRNLEYjaeSTY_NXnr1C9mtv1Kj12FQ7Zo8KrsTpn2IusmcmrBJrVbGZaFVqnZBHEe.mp4?tag=1-1595758298-p-0-5967be10b83044ab-3d4b358efb604372&tt=swft&di=7b0fa029&bp=10081";
        DecryptUrl decryptUrl = new DecryptUrl();
        System.out.println("flag: " + decryptUrl.evaluate(url));
    }
}
