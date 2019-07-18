package rik.direct;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DirectProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DirectProducer.class);
    private static int noOfMessages = 1000;
    private static String SERVICE_URL = "pulsar://localhost:6650";
    private String topicName = "persistent://public/default/test-";
    private static List<String> messages;

    private DirectProducer(String arg) {
        topicName += arg;
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("f","filename", true, "Name of file containing message body templates");
        options.addRequiredOption("t", "topics", true, "csv of topic numbers");
        options.addOption("r", "repetitions", true, "Number of messages to send to each topic");
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

            // set number of messages from r= parameter
            if (cmd.hasOption('r')) {
                try {
                    noOfMessages = Integer.parseInt(cmd.getOptionValue('r'), 10);
                } catch (Exception e) {
                    System.out.println("Unable to parse value for repetitions (r) value, default repetitions [" + noOfMessages +"] used.");
                }
            }

            messages = new ArrayList<>();
            if (cmd.hasOption("f")) {
                ObjectMapper mapper = new ObjectMapper();
                try (InputStream fileStream = new FileInputStream(cmd.getOptionValue("f"))) {
                    List<Message> list = mapper.readValue(fileStream, new TypeReference<List<Message>>() {
                    });
                    for (Message message : list) {
                        for (int i = 0; i < message.getRatio(); i++) {
                            messages.add(message.getBody().asText());
                        }
                    }
                    Collections.shuffle(messages);
                }
            } else {
                messages.add("{\"count\": ${id}}");
            }
            String[] topics = cmd.getOptionValue("t").split(",");
            for (String topic : topics) {
                new Thread(new DirectProducer(topic)).start();
            }
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "DirectProducer", options );
        } catch (IOException e) {
            log.error("Problem with message template file", e);
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
            for (int i = 1; i <= noOfMessages; i++) {
                try {
                    String m = messages.get(i % messages.size()).replace("${id}", String.valueOf(i));
                    producer.send(m.getBytes());
                    // log message at trace for message file
                    log.debug("Sent message: "+ m);
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
