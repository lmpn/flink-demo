package com.ververica.flinktraining.exercises.datatypes;

import java.io.Serializable;

public class QueryResponse implements Serializable {
    public long key;
    public long wm;
    public String response;

    public QueryResponse() {
    }

    public void setQueryResponse( Long queryId, Long wm, String response){
        this.key = queryId;
        this.wm = wm;
        this.response = response;
    }

}
