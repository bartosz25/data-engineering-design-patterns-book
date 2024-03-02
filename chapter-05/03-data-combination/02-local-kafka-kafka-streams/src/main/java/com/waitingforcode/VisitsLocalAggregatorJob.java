package com.waitingforcode;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class VisitsLocalAggregatorJob {

    public static void main(final String[] args) {
        Properties properties = new Properties();
        // against which the application is run.
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "visits-local-aggregator");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "visits-local-aggregator-client");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, String> visitsSource = streamsBuilder.stream("visits");
        KGroupedStream<String, String> groupedVisits = visitsSource
                .groupByKey();
        KStream<String, AggregatedVisits> aggregatedVisits = groupedVisits
                .aggregate(AggregatedVisits::new, new AggregatedVisitsAggregator(),
                        Materialized.with(Serdes.String(), new JsonSerializer<>())) // required to avoid serialization issues
                .toStream();
        aggregatedVisits.to("visits-aggregated", Produced.with(new Serdes.StringSerde(),
                new JsonSerializer<>()));

        Topology aggregatorTopology = streamsBuilder.build();
        System.out.println(aggregatorTopology.describe());

        try (KafkaStreams streams = new KafkaStreams(aggregatorTopology, properties)) {
            streams.start();

            KafkaStreams.State state = streams.state();
            while (state.isRunningOrRebalancing()) {

            }
        }
    }
}
