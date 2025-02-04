package com.waitingforcode;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DataProducerWithRangePartitioner {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.waitingforcode.RangePartitioner");

        Producer<String, String> producer = new KafkaProducer<>(props);
        List<String> lettersToProduce = Arrays.asList("A", "D", "E", "A", "Z");
        lettersToProduce.forEach(letter -> {
            producer.send(new ProducerRecord<>("letters", letter, "Value="+letter));
        });
        producer.close();
    }

}
