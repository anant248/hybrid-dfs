package com.uiuc.systems;

import java.io.Serializable;

public class TupleAck implements Serializable{
    private String tupleId;

    public TupleAck(String tupleId) {
        this.tupleId = tupleId;
    }

    public String getTupleId() {
        return tupleId;
    }

    public void setTupleId(String tupleId) {
        this.tupleId = tupleId;
    }
}
