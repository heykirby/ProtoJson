package com.kuaishou.data.udf.video.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/* copy from kuaishou-flink-media
 * niesipin
 * 20201109 */
public class JsonUtils {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static ObjectReader reader() {
        return mapper.reader();
    }

    public static ObjectWriter writer() {
        return mapper.writer();
    }

    public static ObjectMapper mapper() {
        return mapper;
    }

    public static JsonNode parse(String s) throws IOException {
        return mapper.readTree(s);
    }

    // never returns null, returns MissingNode instead
    public static JsonNode parseSafe(String s) {
        try {
            return mapper.readTree(s);
        } catch (Exception ex) {
            logger.error("parse json error. raw: {}", s);
        }
        return MissingNode.getInstance();
    }

    public static <T> T deserialize(String s, TypeReference typeRef) throws Exception {
        return mapper.readValue(s, typeRef);
    }

    public static <T> T deserialize(String s, Class<T> clazz) throws Exception {
        return mapper.readValue(s, clazz);
    }

    public static String writeValueAsString(Object obj) throws Exception {
        return mapper.writeValueAsString(obj);
    }

    public static byte[] writeValueAsBytes(Object object) throws Exception {
        return mapper.writeValueAsBytes(object);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }

    /**
     * 展开json
     *
     * @param raw
     * @return
     * @throws IOException
     */
    public static JsonNode expandJson(String raw) throws IOException {
        JsonNode node = mapper.readTree(raw);
        if (node != null && node.isObject()) {
            Iterator<String> names = node.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                JsonNode childNode = node.get(name);
                if (childNode.isTextual()) {
                    try {
                        JsonNode expanded = JsonUtils.parse(childNode.asText());
                        ((ObjectNode) node).set(name, expanded);
                    } catch (Exception ex) {
//                         not json string
//                        logger.warn("json expand error: field: {}, value: {}, error: {} ", name,
//                                childNode.asText(""), ex.getMessage());
                    }
                }
            }
        }
        return node;
    }

    public static class JsonPointerDescriptor implements Serializable {
        private String name;
        private String pointer;
        private String type;
        private Object defaultVal;


        public JsonPointerDescriptor(String name, String pointer, String type, Object defaultVal) {
            this.name = name;
            this.pointer = pointer;
            this.type = type;
            this.defaultVal = defaultVal;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPointer() {
            return pointer;
        }

        public void setPointer(String pointer) {
            this.pointer = pointer;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Object getDefaultVal() {
            return defaultVal;
        }

        public void setDefaultVal(Object defaultVal) {
            this.defaultVal = defaultVal;
        }
    }
}
