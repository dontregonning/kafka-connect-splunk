package com.splunk.kafka.connect;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.*;

import java.util.Properties;

import org.junit.Test;

public class KafkaRecordHeadersTest {

    @Test
    public void ProducerRecordTests() {
        ProducerRecord producerRecordNoHead = new ProducerRecord("header-test-1", "key-1", "value-1");

        ProducerRecord producerRecordWithHead = new ProducerRecord("header-test-1", "key-2", "value-2");

        String value = "This is the value";
        byte[] byteArray = value.getBytes();
        Headers headers = producerRecordWithHead.headers().add("header-1", byteArray).add("header-2", byteArray);

        System.out.println("Header Testing");
        System.out.println(producerRecordWithHead.toString());
        System.out.println(producerRecordNoHead.toString());


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        producer.send(producerRecordNoHead);
        producer.send(producerRecordNoHead);
        producer.send(producerRecordNoHead);
        producer.send(producerRecordNoHead);
        producer.send(producerRecordNoHead);


        //producer.send(producerRecordWithHead);

        producer.close();

    }

}
