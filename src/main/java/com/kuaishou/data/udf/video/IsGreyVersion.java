package com.kuaishou.data.udf.video;

import com.kuaishou.data.udf.video.utils.CommonKconfConfig;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

public class IsGreyVersion extends UDF {

    public boolean evaluate(String product, String version){
        HashMap<String, HashMap<String, Boolean>> configs = CommonKconfConfig.GREY_VERSION_CONFIG.get();
        return configs.containsKey(product) && configs.get(product).getOrDefault(version, false);
    }

}
