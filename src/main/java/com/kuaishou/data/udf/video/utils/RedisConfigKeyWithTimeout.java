package com.kuaishou.data.udf.video.utils;

import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.kuaishou.framework.jedis.JedisClusterConfig;

import kuaishou.common.BizDef;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-09-02
 */
public class RedisConfigKeyWithTimeout  implements JedisClusterConfig {

    private JedisClusterConfig config;
    private Long timeout;

    public RedisConfigKeyWithTimeout(JedisClusterConfig config, Long timeout) {
        this.config = config;
        this.timeout = timeout;
    }

    @Override
    public String bizName() {
        return config.bizName();
    }

    @Nonnull
    @Override
    public BizDef bizDef() {
        return config.bizDef();
    }

    @Override
    public boolean usingNewZk() {
        return config.usingNewZk();
    }

    @Override
    public Supplier<Long> operationTimeout() {
        return () -> timeout;
    }
}