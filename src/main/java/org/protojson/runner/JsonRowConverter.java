package org.protojson.runner;

import java.io.IOException;
import java.util.Stack;

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
    private JsonNode[] row;
    private String[] result;
    private String[] EMPTY_RESULT;
    private Stack<JsonToken> stack = new Stack<>();
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
        _json = json;
        if (json == null || json.length() == 0) {
            return EMPTY_RESULT;
        }
        JsonParser parser = MAPPER.createParser(json);
        parser.nextToken();
        _recursion(root, parser);
        return getResult();
    }

    private void _recursion(JsonNode ptr, JsonParser parser) {
        try {
            while (parser.currentToken() != null) {
                //                System.out.println("parser name:" + parser.currentName());
                //                System.out.println("parser token:" + parser.getCurrentToken().name());
                //                System.out.println("node name:" + ptr.getName());
                switch (parser.currentToken()) {
                    case START_OBJECT:
                        ptr.setVersion(currentVersion);
                        ptr.setStart(parser.currentLocation().getColumnNr() - 2);
                        stack.push(JsonToken.START_OBJECT);
                        parser.nextToken();
                        _recursion(ptr, parser);
                        return;
                    case END_OBJECT:
                        ptr.setEnd(parser.getCurrentLocation().getColumnNr() - 1);
                        if (ptr.isLeaf()) {
                            ptr.setValue(_json.substring(ptr.getStart(), ptr.getEnd()));
                        }
                        parser.nextToken();
                        if (stack.size() <= 0 || stack.peek() != JsonToken.START_OBJECT) {
                            throw new RuntimeException("parse error");
                        }
                        stack.pop();
                        return;
                    case START_ARRAY:
                        ptr.setVersion(currentVersion);
                        ptr.setStart(parser.getCurrentLocation().getColumnNr() - 2);
                        stack.push(JsonToken.START_ARRAY);
                        parser.nextToken();
                        int i = 0;
                        while (parser.currentToken() != JsonToken.END_ARRAY) {
                            if (ptr.getChildren().containsKey("" + i)) {
                                ptr = ptr.getChildren().get("" + i);
                                _recursion(ptr, parser);
                                ptr = ptr.getParent();
                            } else {
                                skip(parser);
                            }
                            ++i;
                        }
                        break;
                    case END_ARRAY:
                        ptr.setEnd(parser.getCurrentLocation().getColumnNr() - 1);
                        if (ptr.isLeaf()) {
                            ptr.setValue(_json.substring(ptr.getStart(), ptr.getEnd()));
                        }
                        parser.nextToken();
                        if (stack.size() <= 0 || stack.peek() != JsonToken.START_ARRAY) {
                            throw new RuntimeException("parse error");
                        }
                        stack.pop();
                        return;
                    case FIELD_NAME:
                        parser.nextToken();
                        if (ptr.getChildren() != null && ptr.getChildren().containsKey(parser.getCurrentName())) {
                            ptr = ptr.getChildren().get(parser.getCurrentName());
                            ptr.setVersion(currentVersion);
                            if (ptr.getChildren() == null) {
                                ptr.setValue(skip(parser, true));
                            } else {
                                _recursion(ptr, parser);
                            }
                            ptr = ptr.getParent();
                        } else {
                            skip(parser);
                        }
                        break;
                    case VALUE_STRING:
                    case VALUE_NUMBER_FLOAT:
                    case VALUE_NULL:
                    case VALUE_NUMBER_INT:
                    case VALUE_TRUE:
                    case VALUE_FALSE:
                        ptr.setVersion(currentVersion);
                        ptr.setValue(parser.getValueAsString());
                        return;
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
            result[i] = row[i].getValue();
        }
        return result;
    }

    private String skip(JsonParser parser, boolean retainValue) throws IOException {
        int i = 0, start;
        start = retainValue ? parser.getCurrentLocation().getColumnNr() - 2 : 0;
        String result;
        switch (parser.currentToken()) {
            case START_OBJECT:
                i++;
                while (i > 0 && parser.nextToken() != null) {
                    if (parser.currentToken() == JsonToken.START_OBJECT) {
                        i++;
                    } else if (parser.currentToken() == JsonToken.END_OBJECT) {
                        i--;
                    }
                }
                result = retainValue ? _json.substring(start, parser.getCurrentLocation().getColumnNr() - 1) : null;
                break;
            case START_ARRAY:
                i++;
                while (i > 0 && parser.nextToken() != null) {
                    if (parser.currentToken() == JsonToken.START_ARRAY) {
                        i++;
                    } else if (parser.currentToken() == JsonToken.END_ARRAY) {
                        i--;
                    }
                }
                result = retainValue ? _json.substring(start, parser.getCurrentLocation().getColumnNr() - 1) : null;
                break;
            default:
                result = retainValue ? parser.getValueAsString() : null;
                break;
        }
        parser.nextToken();
        return result;
    }

    private void skip(JsonParser parser) throws IOException {
        skip(parser, false);
    }
}
