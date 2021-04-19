package com.kuaishou.data.udf.video.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kuaishou.kconf.client.Kconf;
import com.kuaishou.kconf.client.Kconfs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CommonKconfConfig {

    public static final Kconf<HashMap<String, HashMap<String, Boolean>>> GREY_VERSION_CONFIG =
            Kconfs.ofJson("videoData.list.GreyVersionConfig",
                    JsonUtils.getMapper().createObjectNode(),
                    ObjectNode.class)
                    .mapper(node -> {
                        HashMap<String, HashMap<String, Boolean>> configs = new HashMap<>();
                        Iterator<Map.Entry<String, JsonNode>> children = node.fields();
                        while (children.hasNext()){
                            Map.Entry<String, JsonNode> child = children.next();
                            JsonNode versions = child.getValue();
                            Iterator<Map.Entry<String, JsonNode>> versionsChild = versions.fields();
                            HashMap<String, Boolean> curKpnConfig = new HashMap<>();
                            while (versionsChild.hasNext()){
                                Map.Entry<String, JsonNode> versionAndGrey = versionsChild.next();
                                curKpnConfig.put(versionAndGrey.getKey(), versionAndGrey.getValue().asBoolean());
                            }
                            configs.put(child.getKey(), curKpnConfig);
                        }
                        return configs;
                    }).build();

}
