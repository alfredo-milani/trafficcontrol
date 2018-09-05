package it.uniroma2.sdcc.sensorssimulator;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;


public class StartProducer {

    private static int threads = 2;
    private static int waitingTimeMillis = 2 * 1000;
    private static ProducerType producerType = ProducerType.AUTO;
    private static SensorType sensorType;
    // File di configurazione
    private final static Config config;
    static {
        config = Config.getInstance();
        try {
            // Caricamento proprietà
            config.loadIfHasNotAlreadyBeenLoaded();
        } catch (IOException e) {
            System.err.println(String.format(
                    "%s: error while reading configuration file",
                    StartProducer.class.getSimpleName()
            ));
            e.printStackTrace();
        }
    }

    private enum ProducerType {
        UNKNOWN,
        KEY,
        AUTO
    }

    private enum SensorType {
        UNKNOWN,
        SEMAPHORE,
        MOBILE
    }

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws Exception {
        parseArgs(args);

        switch (producerType) {
            case KEY:
                switch (sensorType) {
                    case SEMAPHORE:
                        SemaphoreSensorProducer semaphoreSensorProducer = new SemaphoreSensorProducer(
                                new KafkaProducer<>(initProducerProperties()),
                                GENERIC_TUPLE_TO_VALIDATE
                        );
                        BufferedReader bufferedReaderSemaphore = new BufferedReader(new InputStreamReader(System.in));
                        System.out.println(String.format(
                                "Starting producer\nClicca invio per inviare una tupla sul topic <%s>",
                                GENERIC_TUPLE_TO_VALIDATE
                        ));
                        while (true) {
                            try {
                                bufferedReaderSemaphore.readLine();
                                System.out.println(String.format(
                                        "\t> Tupla inviata\t\t| %s |",
                                        semaphoreSensorProducer.produce()
                                ));
                            } catch (IOException e) {
                                System.err.println("Errore durante l'invio della tupla");
                            }
                        }

                    case MOBILE:
                        MobileSensorProducer mobileSensorProducer = new MobileSensorProducer(
                                new KafkaProducer<>(initProducerProperties()),
                                GENERIC_TUPLE_TO_VALIDATE
                        );
                        BufferedReader bufferedReaderMobile = new BufferedReader(new InputStreamReader(System.in));
                        System.out.println(String.format(
                                "Starting producer\nClicca invio per inviare una tupla sul topic <%s>",
                                GENERIC_TUPLE_TO_VALIDATE
                        ));
                        while (true) {
                            try {
                                bufferedReaderMobile.readLine();
                                System.out.println(String.format(
                                        "\t> Tupla inviata\t\t| %s |",
                                        mobileSensorProducer.produce()
                                ));
                            } catch (IOException e) {
                                System.err.println("Errore durante l'invio della tupla");
                            }
                        }
                }

            case AUTO:
                switch (sensorType) {
                    case SEMAPHORE:
                        for (int i = 0; i < threads; ++i) {
                            new Thread(new SemaphoreSensorProducer(
                                    new KafkaProducer<>(initProducerProperties()),
                                    GENERIC_TUPLE_TO_VALIDATE,
                                    waitingTimeMillis)
                            ).start();
                        }
                        break;

                    case MOBILE:
                        for (int i = 0; i < threads; ++i) {
                            new Thread(new MobileSensorProducer(
                                    new KafkaProducer<>(initProducerProperties()),
                                    GENERIC_TUPLE_TO_VALIDATE,
                                    waitingTimeMillis)
                            ).start();
                        }
                        break;
                }

        }
    }

    private static Properties initProducerProperties() {
        Properties producerProperties = new Properties();
        /**
         *  {@link KAFKA_IP_PORT} proprietà collegata ad un altro modulo
         *  attraverso la classe {@link ApplicationsProperties}
         */
        producerProperties.put(BOOTSTRAP_SERVERS, config.getKafkaIpPort());
        producerProperties.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        producerProperties.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        return producerProperties;
    }

    private static void parseArgs(String[] args) throws Exception {
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
                "Modalità emissione tuple (auto - con threads - o key - su richiesta -; default: auto)"
        );
        producerTypeOption.setRequired(false);
        options.addOption(producerTypeOption);

        String sensorType = "st";
        String sensorTypeLong = "sensorTpye";
        Option sensorTypeOption = new Option(
                sensorType,
                String.format("%s=", sensorTypeLong),
                true,
                "Tipo di sensori emulati (semaphore / mobile)"
        );
        sensorTypeOption.setRequired(true);
        options.addOption(sensorTypeOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp(
                    String.format(
                            "StartProducer -%s [sensorType]",
                            sensorType
                    ),
                    options
            );

            System.exit(config.getExitFailure());
            return;
        }

        threads = cmd.getOptionValue(nThreads) == null ?
                threads : Integer.valueOf(cmd.getOptionValue(nThreads));
        waitingTimeMillis = cmd.getOptionValue(waitingTime) == null ?
                waitingTimeMillis : Integer.valueOf(cmd.getOptionValue(waitingTime));
        StartProducer.producerType = cmd.getOptionValue(producerType) == null ?
                ProducerType.AUTO : ProducerType.valueOf(cmd.getOptionValue(producerType).toUpperCase());
        StartProducer.sensorType = cmd.getOptionValue(sensorType) == null ?
                SensorType.UNKNOWN : SensorType.valueOf(cmd.getOptionValue(sensorType).toUpperCase());
        if (StartProducer.sensorType.equals(SensorType.UNKNOWN)) {
            throw new Exception("Devi specificare il tipo di sensore");
        }
    }

}
