package com.github.nizhawan.nitin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiPartitionProducerConsumerTest {
    @Rule
    public KafkaContainer kafka = new KafkaContainer()
            .withLogConsumer(new StdOutConsumer())
            .withEmbeddedZookeeper();

    @Test
    public void testSingleConsumer() throws Exception {
        String topicName = "Integers";
        createTopic(topicName,1);

        NumProducer numProducer = new NumProducer(kafka.getBootstrapServers(), topicName);
        numProducer.start();
        NumConsumer numConsumer = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer.start();

        numProducer.join();
        numConsumer.join();


        assertEquals("should have the value produced by producer ", numProducer.sum(), numConsumer.sum());

    }
    @Test
    public void testTwoConsumers() throws Exception {
        String topicName = "Integers";
        createTopic(topicName,2);

        NumProducer numProducer = new NumProducer(kafka.getBootstrapServers(), topicName);
        numProducer.start();
        NumConsumer numConsumer1 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer1.start();
        NumConsumer numConsumer2 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer2.start();
        numProducer.join();
        numConsumer1.join();
        numConsumer2.join();


        assertEquals("should have the value produced by producer ", numProducer.sum(), numConsumer1.sum()+numConsumer2.sum());

    }

    @Test
    public void testThreeConsumers() throws Exception {
        String topicName = "Integers";
        createTopic(topicName,2);

        NumProducer numProducer = new NumProducer(kafka.getBootstrapServers(), topicName);
        numProducer.start();
        NumConsumer numConsumer1 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer1.start();
        NumConsumer numConsumer2 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer2.start();
        NumConsumer numConsumer3 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer3.start();
        numProducer.join();
        numConsumer1.join();
        numConsumer2.join();
        numConsumer3.join();


        assertEquals("should have the value produced by producer ", numProducer.sum(), numConsumer1.sum()+numConsumer2.sum()+numConsumer3.sum());
        assertEquals("at least one of them did not get any event ",0,numConsumer1.sum()*numConsumer2.sum()*numConsumer3.sum());
    }

    private void createTopic(String topicName,int partitions) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        properties.put("bootstrap.servers", kafka.getBootstrapServers());
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = KafkaAdminClient.create(properties)) {
            NewTopic topic = new NewTopic(topicName, partitions, (short) 1);
            client.createTopics(Collections.singletonList(topic));
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            assertTrue("Topic should have CustomerCountry", names.contains("Integers"));

        }
    }
    @Test
    public void testTwoGroups() throws Exception {
        String topicName = "Integers";
        createTopic(topicName,2);

        NumProducer numProducer = new NumProducer(kafka.getBootstrapServers(), topicName);
        numProducer.start();
        NumConsumer numConsumer11 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer11.start();
        NumConsumer numConsumer12 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group1");
        numConsumer12.start();
        NumConsumer numConsumer21 = new NumConsumer(kafka.getBootstrapServers(), topicName,"group2");
        numConsumer21.start();

        numProducer.join();
        numConsumer11.join();
        numConsumer12.join();
        numConsumer21.join();


        assertEquals("should have the value produced by producer ", numProducer.sum(), numConsumer11.sum()+numConsumer12.sum());
        assertEquals("should have the value produced by producer ", numProducer.sum(), numConsumer21.sum());

    }
    static class NumConsumer extends Thread {
        String topic;
        KafkaConsumer<String, String> consumer;
        long consumedSum = 0;

        public NumConsumer(String bootstrapServers, String topic, String groupId) {
            this.topic = topic;
            Properties consumerProperties = new Properties();
            consumerProperties.put("bootstrap.servers", bootstrapServers);
            consumerProperties.put("group.id", groupId);
            consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<String,
                    String>(consumerProperties);
        }

        public long sum() {
            return consumedSum;
        }

        private void consumeValues() {

            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                String value = record.value();
                consumedSum += Integer.parseInt(value);
                System.out.println(record.key() + "," + record.value() + "," + record.offset()+","+consumer.assignment()+","+consumer);
            }

        }

        public void run() {
            consumer.subscribe(Collections.singletonList(topic));
            consumer.poll(1000);
            consumer.seekToBeginning(consumer.assignment());
            try {
                long lastUpdate = System.currentTimeMillis();
                long oldSum = consumedSum;
                while (System.currentTimeMillis() - lastUpdate < 4000) {
                    consumeValues();
                    if(oldSum != consumedSum){
                        lastUpdate = System.currentTimeMillis();
                        oldSum = consumedSum;
                    }
                }
            } finally {
                consumer.close();
            }
        }
    }

    static class NumProducer extends Thread {
        String topic;
        long producedSum = 0;
        KafkaProducer<String, String> producer;
        Random random;

        public NumProducer(String bootstrapServers, String topic) {
            this.topic = topic;
            Properties producerProperties = new Properties();
            producerProperties.put("bootstrap.servers", bootstrapServers);
            producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producer = new KafkaProducer<String, String>(producerProperties);
            random = new Random();
        }

        public long sum() {
            return producedSum;
        }

        public void run() {
            try {
                for (int i = 0; i < 100; i++) {
                    Integer value = random.nextInt();
                    producedSum += value;
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value + "");
                    producer.send(producerRecord);
                    Thread.sleep(500);
                }
            } catch (Exception e) {

            }
        }
    }
}
