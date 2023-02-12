package org.protojson.pojo;

import java.util.HashMap;

/**
 * @author heye <yehe@kuaishou.com>
 * Created on 2022-10-21
 */
public class JsonNode extends AbstractNode<String, String, JsonNode> {
    private long start = -1;
    private long end = -1;
    public JsonNode(String name) {
        super(name);
    }

    public void _init() {
        setChildren(new HashMap<String, JsonNode>());
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }
}
