package com.waitingforcode;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.HashMap;
import java.util.Map;

public class RangePartitioner implements Partitioner {

    private static final int DEFAULT_PARTITION = 4;
    private final static Map<String, Integer> RANGES_PER_PARTITIONS = new HashMap<>();
    static {
        RANGES_PER_PARTITIONS.put("A", 0);
        RANGES_PER_PARTITIONS.put("B", 0);
        RANGES_PER_PARTITIONS.put("C", 1);
        RANGES_PER_PARTITIONS.put("D", 1);
        RANGES_PER_PARTITIONS.put("E", 2);
        RANGES_PER_PARTITIONS.put("G", 2);
        RANGES_PER_PARTITIONS.put("H", 3);
        RANGES_PER_PARTITIONS.put("I", 3);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String keyAsString = key.toString();
        return RANGES_PER_PARTITIONS.getOrDefault(keyAsString, DEFAULT_PARTITION);
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
