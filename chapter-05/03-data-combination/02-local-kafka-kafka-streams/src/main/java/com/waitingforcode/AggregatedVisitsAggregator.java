package com.waitingforcode;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.streams.kstream.Aggregator;

public class AggregatedVisitsAggregator implements Aggregator<String, String, AggregatedVisits> {
    @Override
    public AggregatedVisits apply(String key, String value, AggregatedVisits aggregate) {
        try {
            InputVisit inputVisit = JsonMapper.MAPPER.readValue(value, InputVisit.class);
            aggregate.addPage(inputVisit.getPage());
            aggregate.setVisitId(inputVisit.getVisitId());

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return aggregate;
    }
}
