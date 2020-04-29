package com.kuaishou.data.udf.video.utils;

import static com.kuaishou.framework.util.ObjectMapperUtils.fromJSON;
import static com.kuaishou.framework.util.ObjectMapperUtils.toJSON;

import java.util.Collections;
import java.util.Set;

import org.apache.curator.utils.ZKPaths;

import com.kuaishou.framework.config.Config;

public enum StringListConfigKey implements Config.ConfigKey<Set<?>>  {
    esLogWhiteList(Long.class), //id白名单
    esLogWhiteListDeviceId(String.class), // device id白名单
    esLogWhiteListAppVersion(String.class), // 版本白名单
    kwaiCdnIPList(String.class),
    appVersionTopList(String.class);


    private Class<?> targetClass;

    StringListConfigKey(Class<?> targetClass) {
        this.targetClass = targetClass;
    }


    @Override
    public Set<?> defaultValue() {
        return Collections.emptySet();
    }

    @Override
    public String configKey() {
        return ZKPaths.makePath("video/list/", name());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Set<Object> value(String rawValue) throws Exception {
        Set<Object> result = fromJSON(rawValue, Set.class, (Class<Object>) targetClass);

        return Collections.unmodifiableSet(result);
    }

    @Override
    public String serialize(Set<?> value) {
        return toJSON(value);
    }
}
