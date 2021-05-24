package com.kuaishou.data.udf.video;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.data.udf.video.utils.CityHash;
import com.kuaishou.framework.util.CityHash11;

/**
 * @author wuxuexin <wuxuexin@kuaishou.com>
 * Created on 2020-12-14
 */
public class CityHashUDF extends UDF {

    public CityHashUDF() {
    }

    public Long evaluate(String groupName, Long uid) {
        long seed;
        if (NumberUtils.isDigits(groupName)) {
            seed = NumberUtils.toLong(groupName);
        } else {
            seed = groupName.hashCode();
        }

        return CityHash.cityHash64WithSeed(uid, seed);
    }

    public Long evaluate(String deviceId) {
        return getTail(deviceIdHash(deviceId), 100);
    }

    public boolean evaluate(String deviceId, Long lowTailNumbers, Long highTailNumbers){
        long tailNumber = getTail(deviceIdHash(deviceId), 100);
        return lowTailNumbers <= tailNumber && tailNumber <= highTailNumbers;
    }

    public static long getTail(long hashed, long mod) {
        if (mod > 0) {
            return ((hashed % mod) + mod) % mod;
        }
        return 0;
    }

    public static long deviceIdHash(String deviceId) {
        return deviceId == null ? 0L : CityHash11.getInstance().cityHash64ASCIIString(deviceId);
    }

}
