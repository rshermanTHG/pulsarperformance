package rik.direct;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DirectConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DirectConsumer.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private String topicName = "persistent://public/default/datacentre-";
    private static final String SUBSCRIPTION_NAME = "test-subscription";

    private DirectConsumer(String args) {
            topicName = topicName + args;
    }

    public static void main(String[] args) {
        for (String arg: args) {
            new Thread(new DirectConsumer(arg)).start();
        }
    }

    @Override
    public void run() {
        try {
            PulsarClient client = new ClientBuilderImpl()
                    .serviceUrl(SERVICE_URL)
                    .build();
            Consumer<byte[]> consumer = client.newConsumer()
                    .topic(topicName)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(SUBSCRIPTION_NAME)
                    .subscribe();
            log.info("Created consumer for the topic {}", topicName);
            int count = 0;
            do {
                Message<byte[]> msg = consumer.receive();
                consumer.acknowledge(msg);
                count++;
                if (count % 10000 == 0) {
                    log.info("Topic: " + topicName + " count: " + count + " message body: " + new String(msg.getData()));
                }
            } while (true);
        } catch (Exception e) {}
    }
}
