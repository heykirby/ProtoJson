package org.simdjson.pojo;

import java.util.HashMap;

/**
 * @author heye
 * Created on 2022-10-21
 */
public abstract class AbstractNode<K, V, C extends AbstractNode> {
    private long version = 0;
    private boolean isLeaf = false;
    private K name;
    private V value = null;
    private C parent = null;
    private HashMap<K, C> children;

    public AbstractNode(K name) {
        this.name = name;
    }
    public abstract void _init();
    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public HashMap<K, C> getChildren() {
        return children;
    }

    public void setChildren(HashMap<K, C> children) {
        this.children = children;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public K getName() {
        return name;
    }

    public void setName(K name) {
        this.name = name;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public C getParent() {
        return parent;
    }

    public void setParent(C parent) {
        this.parent = parent;
    }
}
