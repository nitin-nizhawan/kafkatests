package com.github.nizhawan.nitin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.*;

import static org.junit.Assert.assertTrue;

public class SimpleProducerConsumerTest {
    @Rule
    public KafkaContainer kafka = new KafkaContainer()
            .withLogConsumer(new StdOutConsumer())
            .withEmbeddedZookeeper();

    @Test
    public void testSimpleProducerConsumer() throws Exception {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", kafka.getBootstrapServers());
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = KafkaAdminClient.create(properties)) {
            NewTopic topic = new NewTopic("CustomerCountry", 1, (short) 1);
            client.createTopics(Collections.singletonList(topic));
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            assertTrue("Topic should have CustomerCountry", names.contains("CustomerCountry"));


        }

        produceMessage("testv");

        Properties consumerProperties = new Properties();
        consumerProperties.put("bootstrap.servers", kafka.getBootstrapServers());
        consumerProperties.put("group.id", "CountryCounter");
        consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String,
                String>(consumerProperties);
        consumer.subscribe(Collections.singletonList("CustomerCountry"));


        consumer.poll(1000);
        Map<String, String> custCountryMap = new HashMap<>();

        try {

            consumer.seekToBeginning(consumer.assignment());
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                custCountryMap.put(record.key(), record.value());
                System.out.println(record.key() + "," + record.value() + "," + record.offset());
            }

        } finally {
            consumer.close();
        }
        System.out.println(custCountryMap);
        assertTrue("should have the value produced by producer ", custCountryMap.containsKey("Precision Products"));

    }

    private void produceMessage(String value) {
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", kafka.getBootstrapServers());
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("CustomerCountry", "Precision Products", value);
        try {
            producer.send(producerRecord);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue("Exception while sending msg" + e.getMessage(), false);
        }
    }
}
