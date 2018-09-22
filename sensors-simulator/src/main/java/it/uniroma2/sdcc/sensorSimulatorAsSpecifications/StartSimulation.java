package it.uniroma2.sdcc.sensorSimulatorAsSpecifications;


import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SemaphoresSequencesManager;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;

public class StartSimulation {

    private static long waitingTimeMillis = TimeUnit.SECONDS.toMillis(2);
    private static SensorType sensorType;
    private static long sensorsNum = 50;
    // File di configurazione
    private final static AppConfig APP_CONFIG = AppConfig.getInstance();

    private enum SensorType {
        UNKNOWN,
        SEMAPHORE,
        MOBILE
    }

    @SuppressWarnings("Duplicates")
    public static void main(String[] args)
            throws Exception {
        // Parsing argomenti ricevuto da riga di comando
        parseArgs(args);

        switch (sensorType) {
            case SEMAPHORE:
                // Si potrebbe far generare la sequenza grazie alla classe SemaphoresSequencesManager
                // ma per lo scopo del progetto, è sufficiente simulare coordinate casuali per le coordinate
                // dei semafori (questo vale per tutte le query richieste dal progetto meno che per le terza query)
                new Thread(new SemaphoreSensorProducer(
                        new KafkaProducer<>(initProducerProperties()),
                        GENERIC_TUPLE_TO_VALIDATE,
                        waitingTimeMillis,
                        sensorsNum
                )).start();
                break;

            case MOBILE:
                // Per la terza query sono necessarie delle sequenze di semafori,
                // quindi si utilizza la classe SemaphoresSequencesManager
                new Thread(new MobileSensorProducer(
                        new KafkaProducer<>(initProducerProperties()),
                        GENERIC_TUPLE_TO_VALIDATE,
                        waitingTimeMillis,
                        sensorsNum,
                        SemaphoresSequencesManager.getInstanceFrom(APP_CONFIG)
                )).start();
                break;

            case UNKNOWN:
                System.err.println("Comando sconosciuto");
                System.exit(APP_CONFIG.getExitFailure());
        }
    }

    @SuppressWarnings("Duplicates")
    private static Properties initProducerProperties() {
        Properties producerProperties = new Properties();

        producerProperties.put(BOOTSTRAP_SERVERS, APP_CONFIG.getKafkaIpPort());
        producerProperties.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        producerProperties.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        return producerProperties;
    }

    @SuppressWarnings("Duplicates")
    private static void parseArgs(String[] args)
            throws Exception {
        Options options = new Options();

        String waitingTimeMillis = "w";
        String waitingTimeMillisLong = "waiting";
        Option waitingTimeMillisOption = new Option(
                waitingTimeMillis,
                String.format("%s=", waitingTimeMillisLong),
                true,
                String.format("Secondi di attesa per l'invio di tuple (default: %d)", StartSimulation.waitingTimeMillis)
        );
        waitingTimeMillisOption.setRequired(false);
        options.addOption(waitingTimeMillisOption);

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

        String configFile = "c";
        String configFileLong = "APP_CONFIG";
        Option configFileOption = new Option(
                configFile,
                String.format("%s=", configFileLong),
                true,
                "Path file di configurazione (default: modulo monitoring-system/resources)"
        );
        configFileOption.setRequired(false);
        options.addOption(configFileOption);

        String sensorsNum = "n";
        String sensorsNumLong = "sensorsNum";
        Option sensorsNumOption = new Option(
                sensorsNum,
                String.format("%s=", sensorsNumLong),
                true,
                "Numero di sensori da simulare"
        );
        configFileOption.setRequired(false);
        options.addOption(sensorsNumOption);

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

            System.exit(APP_CONFIG.getExitFailure());
            return;
        }

        StartSimulation.waitingTimeMillis = cmd.getOptionValue(waitingTimeMillis) == null ?
                StartSimulation.waitingTimeMillis : Long.valueOf(cmd.getOptionValue(waitingTimeMillis));
        StartSimulation.sensorsNum = cmd.getOptionValue(sensorsNum) == null ?
                StartSimulation.sensorsNum : Long.valueOf(cmd.getOptionValue(sensorsNum));
        StartSimulation.sensorType = cmd.getOptionValue(sensorType) == null ?
                StartSimulation.SensorType.UNKNOWN : StartSimulation.SensorType.valueOf(cmd.getOptionValue(sensorType).toUpperCase());
        if (StartSimulation.sensorType.equals(StartSimulation.SensorType.UNKNOWN)) {
            throw new Exception("Devi specificare il tipo di sensore");
        }
        // Controllo se è stato passato un file di configurazione per
        // l'applicazione da linea di comando
        if (cmd.getOptionValue(configFile) != null) APP_CONFIG.load(cmd.getOptionValue(configFile));
        else APP_CONFIG.load();
        System.out.println(APP_CONFIG.toString());
    }

}
