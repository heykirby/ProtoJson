package com.kuaishou.data.udf.video;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.exec.UDF;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.kuaishou.abtest.metric.AbtestMetricUtils;
import com.kuaishou.data.udf.video.utils.RedisConfigKey;
import com.kuaishou.framework.abtest.model.ExpAndGroup;
import com.kuaishou.framework.jedis.JedisClusterByZooKeeper;
import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.KconfBuilder;
import com.kuaishou.kconf.client.Kconfs;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-02
 */
public class RelateAbtestFunction2Week extends UDF {
    private static final HashSet<String> WORLDS = new HashSet<>();
    private static JedisClusterByZooKeeper jedis = RedisConfigKey.AB_KS_CHANGE_LOG.get();
    public static final Kconf<Map<String, String>> accessConf = Kconfs.ofStringMap("videoData.anomaly.access", new HashMap<>()).build();
    public static final Kconf<Long> BUCKET = Kconfs.ofLong("videoData.anomaly.hot_key_bucket", 20).build();
    public static final String AB_LOG = "ab_change_log";
    public static final LoadingCache<String, HashMap<String, HashSet<String>>> CACHE =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(300, TimeUnit.SECONDS)
                    .refreshAfterWrite(300, TimeUnit.SECONDS)
                    .build(new CacheLoader<String, HashMap<String, HashSet<String>>>() {
                        @Override
                        public HashMap<String, HashSet<String>> load(String key) throws Exception {
                            long curTimestamp = System.currentTimeMillis();
                            HashMap<String, HashSet<String>> resultMap = new HashMap<>();
                            for (int i = 0; i < BUCKET.get(); i++) {
                                Map<String, String> dataFromRedis = jedis.get().hgetAll(AB_LOG + i);
                                dataFromRedis.entrySet().stream().forEach(entry -> {
                                    if (curTimestamp - Long.parseLong(entry.getValue()) < 24 * 14 * 3600 * 1000) {
                                        String worldName = entry.getKey().split("#")[0];
                                        String expNames = entry.getKey().split("#")[1];
                                        if (resultMap.containsKey(worldName)) {
                                            resultMap.get(worldName).add(expNames);
                                        } else {
                                            HashSet<String> expNamesSet = new HashSet<>();
                                            expNamesSet.add(expNames);
                                            resultMap.put(worldName, expNamesSet);
                                        }
                                    }
                                });
                            }
                            return resultMap;
                        }
                    });

    static {
        WORLDS.addAll(AbtestMetricUtils.getWorldList("KUAISHOU", null, true));
        WORLDS.addAll(AbtestMetricUtils.getWorldList("NEBULA", null, true));
    }


    public static List<String> evaluate(String product, String deviceId, long userId) {
        List<String> result = new ArrayList<>();
        if ("false".equals(accessConf.get().getOrDefault("offline_udf", "false"))) {
            return result;
        }
        try {
            HashSet<String> worlds = new HashSet<>();
            if (!"KUAISHOU".equals(product) && !"NEBULA".equals(product)) {
                worlds = (HashSet<String>) AbtestMetricUtils.getWorldList(product, null, true);
            }
            worlds.addAll(WORLDS);
            HashMap<String, HashSet<String>> abCache = CACHE.get(AB_LOG);
            worlds.retainAll(abCache.keySet());
            worlds.forEach(world -> {
                Map<String, ExpAndGroup> worldToExpAndGroup =
                        AbtestMetricUtils.getAbtestExpAndGroup(deviceId, userId, Collections.singletonList(world));
                ExpAndGroup expAndGroup = worldToExpAndGroup.get(world);
                if (expAndGroup != null && abCache.containsKey(world) && abCache.get(world)
                        .contains(expAndGroup.getExperiment())) {
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
