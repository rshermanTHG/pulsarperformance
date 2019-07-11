package rik.direct;

import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DirectProducer.class);
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private String topicName = "persistent://public/default/datacentre-";

    private DirectProducer(String arg) {
        topicName += arg;
    }

    public static void main(String[] args) {
        for (String arg : args) {
            new Thread(new DirectProducer(arg)).start();
        }
    }

    @Override
    public void run() {
        try {
            PulsarClient client = new ClientBuilderImpl()
                    .serviceUrl(SERVICE_URL)
                    .build();
            Producer<byte[]> producer = client.newProducer()
                    .topic(topicName)
                    .compressionType(CompressionType.LZ4)
                    .create();
            log.info("Created producer for the topic {}", topicName);

            long start = System.currentTimeMillis();
            for (int i = 1; i <= 1000000; i++) {
                try {
                    producer.send(String.valueOf(i).getBytes());
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
            log.info("Topic:" + topicName + " Time = " + ((System.currentTimeMillis() - start) / 1000) + "s");
            client.close();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}
