package com.github.nizhawan.nitin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertTrue;


public class BasicKafkaTest {

   @Rule
    public KafkaContainer kafka = new KafkaContainer()
           .withLogConsumer(new StdOutConsumer())
           .withEmbeddedZookeeper();

    @Test
    public void testKafka() throws  Exception {
        Properties properties = new Properties();

        properties.put("bootstrap.servers",  kafka.getBootstrapServers());
        properties.put("connections.max.idle.ms", 10000);
        properties.put("request.timeout.ms", 5000);
        try (AdminClient client = KafkaAdminClient.create(properties))
        {
            NewTopic topic = new NewTopic("test1",1,(short)1);
            client.createTopics(Collections.singletonList(topic));
            ListTopicsResult topics = client.listTopics();
            Set<String> names = topics.names().get();
            assertTrue("Topic should have test1",names.contains("test1"));

        }
    }



}
