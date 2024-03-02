package com.waitingforcode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AggregatedVisits {

    private String visitId;

    private List<String> pages = new ArrayList<>();

    public String getVisitId() {
        return visitId;
    }

    public void setVisitId(String visitId) {
        this.visitId = visitId;
    }

    public List<String> getPages() {
        return pages;
    }

    public void setPages(List<String> pages) {
        this.pages = pages;
    }

    public void addPage(String page) {
        this.pages.add(page);
    }
}
