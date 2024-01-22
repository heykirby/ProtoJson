package org.simdjson.pojo;

import java.util.HashMap;

/**
 * @author heye
 * Created on 2022-10-21
 */
public class JsonNode extends AbstractNode<String, String, JsonNode> {
    private int start = -1;
    private int end = -1;
    public JsonNode(String name) {
        super(name);
    }

    public void _init() {
        setChildren(new HashMap<String, JsonNode>());
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }
}
