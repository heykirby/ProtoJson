package com.kuaishou.data.udf.video.utils;

import static com.kuaishou.framework.util.ObjectMapperUtils.fromJSON;

import java.util.Collections;
import java.util.Set;

import com.kuaishou.framework.config.Config;

public enum StringListConfigKey implements Config.ConfigKey<Set<String>> {
    APP_VERSION_TOP_LIST("appVersionTopList"),
    APP_VERSION_WHITE_LIST("appVersionWhiteList"),
    KWAI_CDN_IP_LIST("kwaiCdnIPList"),
    ;

    private String path;

    StringListConfigKey(String path) {
        this.path = path;
    }

    @Override
    public String configKey() {
        return "/com/kuaishou/data/udf/video/list/" + this.path;
    }

    @Override
    public Set<String> value(String rawValue) throws Exception {
        Set<String> result = fromJSON(rawValue, Set.class, String.class);

        return Collections.unmodifiableSet(result);
    }

    @Override
    public Set<String> defaultValue() {
        return Collections.emptySet();
    }
}
