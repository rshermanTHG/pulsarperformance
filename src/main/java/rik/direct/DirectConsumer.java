package rik.direct;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;

public class DirectConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DirectConsumer.class);
    private static String SERVICE_URL = "pulsar://localhost:6650";
    private String topicName = "persistent://public/default/test-";
    private static final String SUBSCRIPTION_NAME = "test-subscription";
    private ObjectMapper objectMapper;

    private DirectConsumer(String args) {
        topicName = topicName + args;
        objectMapper = new ObjectMapper();
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addRequiredOption("t", "topics", true, "csv of topic numbers");
        options.addOption("u", "url", true, "Url for pulsar defaults to 'pulsar://localhost:6650'");
        options.addOption("h", "help", false, "Display usage");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse( options, args);
            if (cmd.hasOption('h')) {
                throw new ParseException("help needed");
            }
            if (cmd.hasOption('u')) {
                SERVICE_URL = cmd.getOptionValue('u');
            }
            String[] topics = cmd.getOptionValue("t").split(",");
            for (String topic : topics) {
                new Thread(new DirectConsumer(topic)).start();
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "DirectProducer", options );
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
                String message = new String(msg.getData());
                // log message at trace level for message log
                log.debug("Received message: "+ message);
                if (count % 100 == 0) {
                    int messageCount = objectMapper.readTree(new StringReader(message)).get("count").asInt();
                    log.info("Topic: " + topicName + " count: " + count + " message count: " + messageCount);
                }
            } while (true);
        } catch (Exception e) {
            log.error("Encountered exception: "+ e);
            e.printStackTrace();
        }
    }
}
