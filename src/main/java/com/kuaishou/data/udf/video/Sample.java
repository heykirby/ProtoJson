package com.kuaishou.data.udf.video;

import java.util.Objects;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.framework.config.util.CityHashTailNumberConfig;
import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.MoreKconfs;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-01
 */
public class Sample extends UDF {
    private static final Kconf<CityHashTailNumberConfig> VOD_CDN_TAIL_NUMBER =
            MoreKconfs.newTailNumberConfig("videoData.flinkFeatureSwitch.vodCdnTailNumber");
    public static long deviceIdHash(String deviceId) {
        return deviceId == null ? (int) (Math.random() * 100 * 100) : Objects.hash(deviceId);
    }
    public static boolean evaluate(String deviceId) {
        return VOD_CDN_TAIL_NUMBER.get().isOnFor(deviceIdHash(deviceId));
    }

    public static void main(String[] args) {
        System.out.println(evaluate("12345"));
    }
}
