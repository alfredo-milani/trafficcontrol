package it.uniroma2.sdcc.sensorssimulator;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.EXIT_FAILURE;
import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;


public class StartProducer {

    private final static String CLASS_NAME = StartProducer.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    private static int threads = 2;
    private static int waitingTimeMillis = 2 * 1000;

    public static void main(String[] args) {
        parseArgs(args);

        Properties producerProperties = new Properties();
        producerProperties.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
        producerProperties.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        producerProperties.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        for (int i = 0; i < threads; ++i) {
            new Thread(new SemaphoreSensorThread(producer, GENERIC_TUPLE_TO_VALIDATE, waitingTimeMillis)).start();
        }
    }

    private static void parseArgs(String[] args) {
        Options options = new Options();

        String nThreads = "t";
        String nThreadsLong = "threads";
        Option numberWorkersOption = new Option(
                nThreads,
                String.format("%s=", nThreadsLong),
                true,
                String.format("Numero di threads nella simulazione (default: %d)", threads)
        );
        numberWorkersOption.setRequired(false);
        options.addOption(numberWorkersOption);

        String waitingTime = "w";
        String waitingTimeLong = "waiting";
        Option waitingTimeOption = new Option(
                waitingTime,
                String.format("%s=", waitingTimeLong),
                true,
                String.format("Secondi di attesa per l'invio di tuple (default: %d)", waitingTimeMillis)
        );
        waitingTimeOption.setRequired(false);
        options.addOption(waitingTimeOption);

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
        waitingTimeMillis = cmd.getOptionValue(waitingTime) == null ?
                waitingTimeMillis : Integer.valueOf(cmd.getOptionValue(waitingTime));
    }

    public static Logger getLOGGER() {
        return LOGGER;
    }
}
