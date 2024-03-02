package com.waitingforcode;

import com.fasterxml.jackson.annotation.JsonProperty;

public class InputVisit {

    @JsonProperty("visit_id")
    private String visitId;
    private String page;

    public String getPage() {
        return page;
    }

    public void setPage(String page) {
        this.page = page;
    }

    public String getVisitId() {
        return visitId;
    }

    public void setVisitId(String visitId) {
        this.visitId = visitId;
    }
}
