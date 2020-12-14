package com.kuaishou.data.udf.video;

import com.kuaishou.data.udf.video.utils.CityHash;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

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

}
