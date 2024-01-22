package org.simdjson;

import org.simdjson.pojo.JsonNode;

/**
 * @author heye
 * Created on 2024-01-22
 * for the following json example:
 * {
 *    "name":"Bill Gates",
 *    "age":62,
 *    "cars": [
 * 	        { "name":"Porsche",  "models":[ "911", "Taycan" ] },
 *          { "name":"BMW", "models":[ "M5", "M3", "X5" ] },
 *          { "name":"Volvo", "models":[ "XC60", "V60" ] }
 *    ]
 * }
 *
 * SimdKsonParser parser = new SimdKsonParser("name", "cars.0", "cars.0.name", "cars.0.models", "cars.1.models.0");
 * String[] result = parser.parse(json);
 * and the result is: [
 *      0 -> "Bill Gates",
 *      1 -> "{ \"name\":\"Porsche\",  \"models\":[ \"911\", \"Taycan\" ] }",
 *      2 -> "Porsche",
 *      3 -> "[ \"911\", \"Taycan\" ]",
 *      4 -> "M5"
 * ]
 */
public class SimdKsonParser {
    private final SimdJsonParser parser;
    private BitIndexes bitIndexes;
    private JsonNode root = new JsonNode("root");
    private JsonNode[] row;
    private String[] result;
    private String[] emptyResult;
    private JsonNode ptr;
    private byte[] buffer;
    private final int targetParseNum;
    private long currentVersion = 0;
    // pruning, when alreadyProcessedCols == NUM
    private long alreadyProcessedCols = 0;

    public SimdKsonParser(String... args) {
        parser = new SimdJsonParser();
        targetParseNum = args.length;
        row = new JsonNode[targetParseNum];
        result = new String[targetParseNum];
        emptyResult = new String[targetParseNum];
        for (int i = 0; i < args.length; i++) {
            emptyResult[i] = null;
        }
        for (int i = 0; i < targetParseNum; i++) {
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

    public String[] parse(byte[] buffer, int len) {
        this.bitIndexes = parser.buildIndex(buffer, len);
        if (buffer == null || buffer.length == 0) {
            return emptyResult;
        }
        this.alreadyProcessedCols = 0;
        this.currentVersion++;
        this.ptr = root;
        this.buffer = buffer;

        switch (buffer[bitIndexes.peek()]) {
            case '{' -> {
                parseMap();
            }
            case '[' -> {
                parseArray();
            }
            default -> {
                throw new RuntimeException("invalid json format");
            }
        }
        return getResult();
    }

    private void parseElement(String name) {
        String key = name;
        if (name == null) {
            int start = bitIndexes.advance();
            int end = bitIndexes.advance();
            int realEnd = end;
            while (realEnd > start) {
                if (buffer[--realEnd] == '"') {
                    break;
                }
            }
            key = new String(buffer, start + 1, realEnd - start - 1);
        }
        if (!ptr.getChildren().containsKey(key)) {
            skip(false);
            return;
        }
        ptr = ptr.getChildren().get(key);
        switch (buffer[bitIndexes.peek()]) {
            case '{' -> {
                parseMap();
            }
            case '[' -> {
                parseArray();
            }
            default -> {
                ptr.setValue(skip(true));
                ptr.setVersion(currentVersion);
                ++alreadyProcessedCols;
            }
        }
        ptr = ptr.getParent();
    }

    private void parseMap() {
        if (ptr.getChildren() == null) {
            ptr.setValue(skip(true));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
            return;
        }
        ptr.setStart(bitIndexes.peek());
        bitIndexes.advance();
        while (bitIndexes.hasNext() && buffer[bitIndexes.peek()] != '}' && alreadyProcessedCols < targetParseNum) {
            parseElement(null);
            if (buffer[bitIndexes.peek()] == ',') {
                bitIndexes.advance();
            }
        }
        ptr.setEnd(bitIndexes.peek());
        if (ptr.isLeaf()) {
            ptr.setValue(new String(buffer, ptr.getStart(), ptr.getEnd() - ptr.getStart() + 1));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
        }
        bitIndexes.advance();
    }

    private void parseArray() {
        if (ptr.getChildren() == null) {
            ptr.setValue(skip(true));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
            return;
        }
        ptr.setStart(bitIndexes.peek());
        bitIndexes.advance();
        int i = 0;
        while (bitIndexes.hasNext() && buffer[bitIndexes.peek()] != ']' && alreadyProcessedCols < targetParseNum) {
            parseElement("" + i);
            if (buffer[bitIndexes.peek()] == ',') {
                bitIndexes.advance();
            }
            i++;
        }
        ptr.setEnd(bitIndexes.peek());
        if (ptr.isLeaf()) {
            ptr.setValue(new String(buffer, ptr.getStart(), ptr.getEnd() - ptr.getStart() + 1));
            ptr.setVersion(currentVersion);
            ++alreadyProcessedCols;
        }
        bitIndexes.advance();
    }

    private String skip(boolean retainValue) {
        int i = 0;
        int start = retainValue ? bitIndexes.peek() : 0;
        switch (buffer[bitIndexes.peek()]) {
            case '{' -> {
                i++;
                while (i > 0) {
                    bitIndexes.advance();
                    if (buffer[bitIndexes.peek()] == '{') {
                        i++;
                    } else if (buffer[bitIndexes.peek()] == '}') {
                        i--;
                    }
                }
                int end = bitIndexes.peek();
                bitIndexes.advance();
                return retainValue ? new String(buffer, start, end - start + 1) : null;
            }
            case '[' -> {
                i++;
                while (i > 0) {
                    bitIndexes.advance();
                    if (buffer[bitIndexes.peek()] == '[') {
                        i++;
                    } else if (buffer[bitIndexes.peek()] == ']') {
                        i--;
                    }
                }
                int end = bitIndexes.peek();
                bitIndexes.advance();
                return retainValue ? new String(buffer, start, end - start + 1) : null;
            }
            case '"' -> {
                bitIndexes.advance();
                int realEnd = bitIndexes.peek();
                while (realEnd > start) {
                    if (buffer[--realEnd] == '"') {
                        break;
                    }
                }
                return retainValue ? new String(buffer, start + 1, realEnd - start - 1) : null;
            }
            default -> {
                bitIndexes.advance();
                int realEnd = bitIndexes.peek();
                while (realEnd >= start) {
                    --realEnd;
                    if (buffer[realEnd] >= '0' && buffer[realEnd] <= '9') {
                        break;
                    }
                }
                return retainValue ? new String(buffer, start, realEnd - start + 1) : null;
            }
        }
    }

    private String[] getResult() {
        for (int i = 0; i < targetParseNum; i++) {
            if (row[i].getVersion() < currentVersion) {
                result[i] = null;
                continue;
            }
            result[i] = row[i].getValue();
        }
        return result;
    }
}
