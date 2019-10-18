package com.github.nizhawan.nitin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KafkaStreamTest {
    @Rule
    public KafkaContainer kafka = new KafkaContainer()
            .withLogConsumer(new StdOutConsumer())
            .withEmbeddedZookeeper();

    @Test
    public void testKafkaStream() throws Exception {
        String topicName = "Integers";
        String targetTopic = "DoubleIntegers";
        createTopic(topicName,4);

        NumProducer numProducer = new NumProducer(kafka.getBootstrapServers(), topicName);
        numProducer.start();

        StreamProcessor streamProcessor = new StreamProcessor(kafka.getBootstrapServers(),topicName,targetTopic);
        streamProcessor.start();

        NumConsumer numConsumer = new NumConsumer(kafka.getBootstrapServers(), targetTopic,"group1");
        numConsumer.start();

        numProducer.join();
        numConsumer.join();


        assertEquals("should have the value produced by producer ", 2*numProducer.sum(), numConsumer.sum());

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
    static class StreamProcessor extends  Thread {
        KafkaStreams kafkaStreams;
        public StreamProcessor(String bootstrapServers, String sourceTopic, String targetTopic){
            final Properties streamsConfiguration = new Properties();
            // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
            // against which the application is run.
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "simple-kafka-stream");

            // Where to find Kafka broker(s).
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            // Specify default (de)serializers for record keys and for record values.
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            StreamsBuilder streamBuilder = new StreamsBuilder();

            KStream<String,String> kstream = streamBuilder.stream(sourceTopic);

            KStream<String,String> newStream = kstream.mapValues( s -> (Integer.parseInt(s)*2)+"");

            newStream.to(targetTopic);

            Topology topology = streamBuilder.build();

            kafkaStreams = new KafkaStreams(topology,streamsConfiguration);


        }
        public void run(){
            kafkaStreams.start();
        }
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
                    Integer value = i;
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
