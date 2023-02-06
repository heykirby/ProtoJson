package org.protojson.runner;

import java.io.IOException;

import org.protojson.pojo.JsonNode;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-10-21
 */
public class JsonRowConverter {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private String _json;
    private JsonNode root = new JsonNode("root");
    private JsonNode _ptr = root;
    private JsonNode[] row;
    private String[] result;
    private String[] EMPTY_RESULT;
    private final int NUM;
    private long currentVersion = 0;

    public JsonRowConverter(String... args) {
        NUM = args.length;
        row = new JsonNode[NUM];
        result = new String[NUM];
        EMPTY_RESULT = new String[NUM];
        for (int i = 0; i < args.length; i++) {
            EMPTY_RESULT[i] = null;
        }
        for (int i = 0; i < NUM; i++) {
            JsonNode cur = root;
            String[] paths = args[i].split("\\.");
            for (int j = 0; j < paths.length; j++) {
                if (cur.getChildren() == null) {
                    cur._init();
                }
                if (!cur.getChildren().containsKey(paths[j])) {
                    JsonNode child = new JsonNode(paths[j]);
                    cur.getChildren().put(paths[j], child);
                    child.setParent(cur);
                }
                cur = cur.getChildren().get(paths[j]);
            }
            cur.setLeaf(true);
            row[i] = cur;
        }
    }
    public String[] process(String json) throws IOException {
        currentVersion++;
        _ptr = root;
        _json = json;
        if (json == null || json.length() == 0) {
            return EMPTY_RESULT;
        }
        JsonParser parser = MAPPER.createParser(json);
        parser.nextToken();
        _recursion(_ptr, parser, null);
        return getResult();
    }
    private void _recursion(JsonNode ptr, JsonParser parser, JsonToken skipArray) {
        try {
            while (parser.currentToken() != null) {
                //                System.out.println("parser name:" + parser.currentName());
                //                System.out.println("parser token:" + parser.getCurrentToken().name());
                //                System.out.println("node name:" + ptr.getName());
                switch (parser.currentToken()) {
                    case START_OBJECT:
                        ptr.setVersion(currentVersion);
                        ptr.setStart(parser.currentLocation().getColumnNr() - 2);
                        parser.nextToken();
                        break;
                    case END_OBJECT:
                        ptr.setEnd(parser.getCurrentLocation().getColumnNr() - 1);
                        ptr = ptr.getParent();
                        parser.nextToken();
                        if (skipArray == JsonToken.END_OBJECT) return;
                        break;
                    case START_ARRAY:
                        ptr.setVersion(currentVersion);
                        ptr.setStart(parser.getCurrentLocation().getColumnNr() - 2);
                        int i = 0;
                        while (parser.currentToken() != JsonToken.END_ARRAY) {
                            parser.nextToken();
                            if (ptr.getChildren().containsKey("" + i)) {
                                ptr = ptr.getChildren().get("" + i);
                                if (parser.currentToken() == JsonToken.START_OBJECT) {
                                    _recursion(ptr, parser, JsonToken.END_OBJECT);
                                } else if (parser.currentToken() == JsonToken.START_ARRAY) {
                                    _recursion(ptr, parser, JsonToken.END_ARRAY);
                                } else {
                                    _recursion(ptr, parser, JsonToken.NOT_AVAILABLE);
                                }
                                ptr = ptr.getParent();
                            } else {
                                skip(parser);
                            }
                            ++i;
                        }
                        break;
                    case END_ARRAY:
                        ptr.setEnd(parser.getCurrentLocation().getColumnNr() - 1);
                        ptr = ptr.getParent();
                        parser.nextToken();
                        if (skipArray == JsonToken.END_ARRAY) return;
                        break;
                    case FIELD_NAME:
                        if (ptr.getChildren() != null && ptr.getChildren().containsKey(parser.getCurrentName())) {
                            ptr = ptr.getChildren().get(parser.getCurrentName());
                            ptr.setVersion(currentVersion);
                            if (ptr.getChildren() == null) {
                                parser.nextToken();
                                ptr.setValue(skip(parser,true));
                                ptr = ptr.getParent();
                            }
                        } else {
                            parser.nextToken();
                            skip(parser);
                        }
                        parser.nextToken();
                        break;
                    case VALUE_STRING:
                    case VALUE_NUMBER_FLOAT:
                    case VALUE_NULL:
                    case VALUE_NUMBER_INT:
                    case VALUE_TRUE:
                    case VALUE_FALSE:
                        ptr.setVersion(currentVersion);
                        ptr.setValue(parser.getValueAsString());
                        ptr = ptr.getParent();
                        if (skipArray == JsonToken.NOT_AVAILABLE) return;
                        break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private String[] getResult() {
        for (int i = 0; i < NUM; i++) {
            if (row[i].getVersion() < currentVersion) {
                result[i] = null;
                continue;
            }
            if (row[i].getChildren() != null) {
                result[i] = _json.substring(row[i].getStart(), row[i].getEnd());
            } else {
                result[i] = row[i].getValue();
            }
        }
        return result;
    }
    private String skip(JsonParser parser, boolean flag) throws IOException {
        _ptr.setVersion(currentVersion);
        int i = 0, start;
        start = flag ? parser.getCurrentLocation().getColumnNr() - 2 : 0;
        switch (parser.currentToken()) {
            case START_OBJECT:
                i++;
                while (i > 0 && parser.nextToken() != null) {
                    if (parser.currentToken() == JsonToken.START_OBJECT) i++;
                    else if (parser.currentToken() == JsonToken.END_OBJECT) i--;
                }
                return flag ? _json.substring(start, parser.getCurrentLocation().getColumnNr() - 1): null;
            case START_ARRAY:
                i++;
                while (i > 0 && parser.nextToken() != null) {
                    if (parser.currentToken() == JsonToken.START_ARRAY) i++;
                    else if (parser.currentToken() == JsonToken.END_ARRAY) i--;
                }
                return flag ? _json.substring(start, parser.getCurrentLocation().getColumnNr() - 1): null;
            default:
                return flag ? parser.getValueAsString() : null;
        }
    }
    private void skip(JsonParser parser) throws IOException {
        skip(parser, false);
    }
}
