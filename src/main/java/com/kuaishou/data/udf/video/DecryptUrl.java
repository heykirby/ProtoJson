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
        } catch (Exception ignored) {
            return "invalid_url:" + originUrl;
        }

        return originUrl;
    }

    public static void main(String[] args) {
        String url = "httpkwaiprotocol:http://tx-playback.video.yximgs.com/mediacloud/gzone/game_video/segment/Sr3dUn7gHpn7anZnr5s82VeTEbWG1_mZUbi9N31yg_c.m3u8";
        DecryptUrl decryptUrl = new DecryptUrl();
        System.out.println("flag: " + decryptUrl.evaluate(url));
    }
}
