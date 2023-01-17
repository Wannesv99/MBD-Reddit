package com.temps;

import java.util.UUID;

public class Relationship {
    private String type;
    private String src;
    private String dst;
    // private UUID id;
    private String id;

    public Relationship(String type, String src, String dst) {
        this.type = type;
        this.src = src;
        this.dst = dst;
        // this.id = UUID.randomUUID();
        this.id = "super id";
    }

    public String getType() {
        return type;
    }

    public String getSrc() {
        return src;
    }

    public String getDst() {
        return dst;
    }

    public String getId() {
        return id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setSrc(String src) {
        this.src = src;
    }

    public void setDst(String dst) {
        this.dst = dst;
    }

    public void setId(String id) {
        this.id = id;
    }
}

