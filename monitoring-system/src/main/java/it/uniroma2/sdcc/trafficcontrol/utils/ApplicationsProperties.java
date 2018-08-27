package it.uniroma2.sdcc.trafficcontrol.utils;

import lombok.Cleanup;
import lombok.ToString;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@ToString(onlyExplicitlyIncluded = true)
public class ApplicationsProperties extends Properties {

    // Nome applicazione
    @ToString.Include
    public static final String APP_NAME = "Monitoring System";

    // Exit status
    public static final int EXIT_SUCCESS = 0;
    public static final int EXIT_FAILURE = 1;

    // Nome file proprietà
    private static final String DEFAULT_PROPERTIES_FILENAME = "config.properties";
    @ToString.Include
    public static String PROPERTIES_FILENAME;

    // MODE values
    public static final String MODE_LOCAL = "local";
    public static final String MODE_CLUSTER = "cluster";
    // Kafka default ip address
    public static final String KAFKA_IP_DEFAULT = "localhost";
    // Kafka default port
    public static final Integer KAFKA_PORT_DEFAULT = 9092;
    public static String KAFKA_IP_PORT = KAFKA_IP_DEFAULT + ":" + KAFKA_PORT_DEFAULT;
    // Default workers' number
    public static final Integer NUMBER_WORKERS_DEFAULT = 2;

    // Proprietà "mode"
    private static final String P_MODE = "mode";
    @ToString.Include
    public static String MODE = MODE_LOCAL;
    // Proprietà "kafka-ip"
    private static final String P_KAFKA_IP = "kafka-ip";
    @ToString.Include
    public static String KAFKA_IP = KAFKA_IP_DEFAULT;
    // Proprietà "kafka-port"
    private static final String P_KAFKA_PORT = "kafka-port";
    @ToString.Include
    public static Integer KAFKA_PORT = KAFKA_PORT_DEFAULT;
    // Proprietà "workers"
    private static final String P_NUMBER_OF_WORKERS = "workers";
    @ToString.Include
    public static Integer NUMBER_OF_WORKERS = NUMBER_WORKERS_DEFAULT;
    // Parametri per terza query
    // Proprietà "road-delta"
    // Rappresenta l'errore massimo nell'applicazione che associa
    // una vettuera ad una sequenza di semafori
    private static final String P_ROAD_DELTA = "road-delta";
    @ToString.Include
    public static Double ROAD_DELTA;
    // File (JSON) contenente la descrizione della sequenza di semafori
    private static final String P_SEMAPHORES_SEQUENCES_FILE = "sequences-semaphores-structure";
    @ToString.Include
    public static String SEMAPHORES_SEQUENCES_FILE;



    private static class Cointainer {
        private final static ApplicationsProperties instance = new ApplicationsProperties();
    }

    protected ApplicationsProperties() {

    }

    public static ApplicationsProperties getInstance() {
        return Cointainer.instance;
    }

    public void loadProperties()
            throws IOException {
        loadProperties(DEFAULT_PROPERTIES_FILENAME);
    }

    public void loadProperties(String pathConfigurationFile)
            throws IOException {
        if (pathConfigurationFile != null) {
            PROPERTIES_FILENAME = pathConfigurationFile;
        } else {
            PROPERTIES_FILENAME = DEFAULT_PROPERTIES_FILENAME;
        }

        @Cleanup InputStream input = ApplicationsProperties.class
                .getClassLoader()
                .getResourceAsStream(pathConfigurationFile);

        if (input == null) {
            System.out.println("Sorry, unable to find " + pathConfigurationFile);
            return;
        }

        load(input);

        MODE = getProperty(P_MODE) == null
                ? MODE_LOCAL
                : getProperty(P_MODE);
        KAFKA_IP = getProperty(P_KAFKA_IP);
        KAFKA_PORT = getProperty(P_KAFKA_PORT) == null
                ? KAFKA_PORT_DEFAULT
                : Integer.valueOf(getProperty(P_KAFKA_PORT));
        KAFKA_IP_PORT = String.format("%s:%d", KAFKA_IP, KAFKA_PORT);
        NUMBER_OF_WORKERS = getProperty(P_NUMBER_OF_WORKERS) == null
                ? NUMBER_WORKERS_DEFAULT
                : Integer.valueOf(getProperty(P_NUMBER_OF_WORKERS));
        ROAD_DELTA = getProperty(P_ROAD_DELTA) == null
                ? null
                : Double.valueOf(getProperty(P_ROAD_DELTA));
        SEMAPHORES_SEQUENCES_FILE = getProperty(P_SEMAPHORES_SEQUENCES_FILE);
    }

}
