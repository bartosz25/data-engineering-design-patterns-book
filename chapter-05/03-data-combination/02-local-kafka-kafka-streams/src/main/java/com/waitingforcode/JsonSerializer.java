package com.waitingforcode;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonSerializer<T> implements Serializer<AggregatedVisits>,
        Deserializer<AggregatedVisits>, Serde<AggregatedVisits> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {}

    @Override
    @SuppressWarnings("unchecked")
    public AggregatedVisits deserialize(String topic, byte[] data) {
        try {
            return OBJECT_MAPPER.readValue(data, AggregatedVisits.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final AggregatedVisits data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public byte[] serialize(String topic, Headers headers, AggregatedVisits data) {
        return Serializer.super.serialize(topic, headers, data);
    }

    @Override
    public void close() {}

    @Override
    public Serializer<AggregatedVisits> serializer() {
        return this;
    }

    @Override
    public Deserializer<AggregatedVisits> deserializer() {
        return this;
    }

}