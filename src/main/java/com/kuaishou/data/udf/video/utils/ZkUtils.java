package com.kuaishou.data.udf.video.utils;

import static com.github.phantomthief.zookeeper.util.ZkUtils.getStringFromZk;
import static com.kuaishou.framework.util.ObjectMapperUtils.fromJSON;
import static org.slf4j.LoggerFactory.getLogger;

import java.util.Collections;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.Logger;

public class ZkUtils {

    private static final Logger logger = getLogger(ZkUtils.class);

    private static final String ZK_HOSTS = "zk.cluster.observer.cp";

    private static final int RETRY_DELAY_MS = 100;

    public static String getZkNodeString(String zkPath) {
        logger.info("host {} path {}", ZK_HOSTS, zkPath);
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(ZK_HOSTS,
                new RetryOneTime(RETRY_DELAY_MS))) {
            client.start();
            return getStringFromZk(client, zkPath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<String> getValue(String ZK_PATH) throws Exception {
        String appWorldLis = getZkNodeString("/kuaishou/config/video/list/"+ZK_PATH);
        Set<String> result = fromJSON(appWorldLis, Set.class, String.class);
        return Collections.unmodifiableSet(result);
    }
}
