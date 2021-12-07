package com.kuaishou.data.udf.video.utils;

import java.util.Collections;
import java.util.Set;
import java.util.function.Supplier;

import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.Kconfs;

public enum VideoStringListConfig implements Supplier<Set<String>> {

    kwaiCdnIPList;

    private final Kconf<Set<String>> info;

    VideoStringListConfig() {
        info = Kconfs.ofStringSet("videoData.list." + name(), Collections.emptySet()).build();
    }

    VideoStringListConfig(String key) {
        info = Kconfs.ofStringSet(key, Collections.emptySet()).build();
    }

    @Override
    public Set<String> get() {
        return info.get();
    }
}
