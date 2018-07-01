package it.uniroma2.sdcc.sensorssimulator;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.EXIT_FAILURE;
import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;


public class StartProducer {

    private static int threads = 2;
    private static int waitingTimeMillis = 2 * 1000;
    private static ProducerType producerType = ProducerType.AUTO;

    private enum ProducerType {
        KEY,
        AUTO
    }

    public static void main(String[] args) {
        parseArgs(args);

        switch (producerType) {
            case KEY:
                SemaphoreSensorProducer semaphoreSensorProducer = new SemaphoreSensorProducer(
                        new KafkaProducer<>(initProducerProperties()),
                        GENERIC_TUPLE_TO_VALIDATE
                );
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
                System.out.println(String.format(
                        "Starting producer\nClicca invio per inviare una tupla sul topic <%s>",
                        GENERIC_TUPLE_TO_VALIDATE
                ));
                while (true) {
                    try {
                        bufferedReader.readLine();
                        System.out.println(String.format(
                                "\t> Tupla inviata\t\t| %s |",
                                semaphoreSensorProducer.produce()
                        ));
                    } catch (IOException e) {
                        System.err.println("Errore durante l'invio della tupla");
                    }
                }

            case AUTO:
                for (int i = 0; i < threads; ++i) {
                    new Thread(new SemaphoreSensorProducer(
                            new KafkaProducer<>(initProducerProperties()),
                            GENERIC_TUPLE_TO_VALIDATE,
                            waitingTimeMillis)
                    ).start();
                }
                break;
        }
    }

    private static Properties initProducerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
        producerProperties.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        producerProperties.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        return producerProperties;
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

        String producerType = "p";
        String producerTypeLong = "producerType";
        Option producerTypeOption = new Option(
                producerType,
                String.format("%s=", producerTypeLong),
                true,
                "Modalità emissione tuple (auto - con threads o key - su richiesta)"
        );
        producerTypeOption.setRequired(false);
        options.addOption(producerTypeOption);

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
        StartProducer.producerType = cmd.getOptionValue(producerType) == null ?
                ProducerType.AUTO : ProducerType.valueOf(cmd.getOptionValue(producerType).toUpperCase());
    }

}
