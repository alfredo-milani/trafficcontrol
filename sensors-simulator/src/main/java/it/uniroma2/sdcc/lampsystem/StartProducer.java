package it.uniroma2.sdcc.lampsystem;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.EXIT_FAILURE;
import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;


public class StartProducer {

    private static int threads = 2;

    public static void main(String[] args) {
        parseArgs(args);

        Properties producerProperties = new Properties();
        producerProperties.put(SERVER, KAFKA_IP_PORT);
        producerProperties.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        producerProperties.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(producerProperties);

        for (int i = 0; i < threads; ++i) {
            new Thread(new SemaphoreSensorThread(producer, MONITORING_SOURCE)).start();
        }
    }

    private static void parseArgs(String[] args) {
        Options options = new Options();

        String nThreads = "t";
        String nThreadsLong = "threads";
        Option numberWorkers = new Option(
                nThreads,
                String.format("%s=", nThreadsLong),
                true,
                String.format("Numero di threads nella simulazione (default: %d)", threads)
        );
        numberWorkers.setRequired(false);
        options.addOption(numberWorkers);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp(
                    String.format(
                            "StartProducer -%s [num threads]",
                            nThreads
                    ),
                    options
            );

            System.exit(EXIT_FAILURE);
            return;
        }

        threads = cmd.getOptionValue(nThreads) == null ?
                threads : Integer.valueOf(cmd.getOptionValue(nThreads));
    }

}
