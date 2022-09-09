package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.kuaishou.abtest.metric.AbtestMetricUtils;
import com.kuaishou.framework.abtest.model.ExpAndGroup;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-02
 */
public class RelateAbtestFunction extends UDF {
    private static final HashSet<String> WORLDS = new HashSet<>();

    static {
        WORLDS.addAll(AbtestMetricUtils.getWorldList("KUAISHOU", null, true));
        WORLDS.addAll(AbtestMetricUtils.getWorldList("NEBULA", null, true));
    }


    public static List<String> evaluate(String product, String deviceId, long userId) {
        ArrayList<String> result = new ArrayList<>();
        try {
            HashSet<String> worlds = new HashSet<>();
            if (!"KUAISHOU".equals(product) && !"NEBULA".equals(product)) {
                worlds = (HashSet<String>) AbtestMetricUtils.getWorldList(product, null, true);
            }
            worlds.addAll(WORLDS);
            worlds.forEach(world -> {
                Map<String, ExpAndGroup> worldToExpAndGroup =
                        AbtestMetricUtils.getAbtestExpAndGroup(deviceId, userId, Collections.singletonList(world));
                ExpAndGroup expAndGroup = worldToExpAndGroup.get(world);
                if (expAndGroup != null) {
                    result.add(world + "." + expAndGroup.getExperiment() + "." + expAndGroup.getGroup());
                }
            });
        } catch (Exception e) {
        }
        return result;
    }

    public static void main(String[] args) {
        System.out.println(evaluate("KUAISHOU", "ANDROID_f1ad61e3d39bc267", 1261147791));
    }
}
