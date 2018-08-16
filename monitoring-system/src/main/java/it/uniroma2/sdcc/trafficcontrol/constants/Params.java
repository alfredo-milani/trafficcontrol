package it.uniroma2.sdcc.trafficcontrol.constants;

public class Params {

    // Nome applicazione
    public final static String APP_NAME = "Monitoring system";

    // Exit status
    public final static int EXIT_SUCCESS = 0;
    public final static int EXIT_FAILURE = 1;


    public static class Properties {

        // Nome file proprietà
        public static final String PROPERTIES_FILENAME = "config.properties";

        // MODE values
        public static final String MODE_LOCAL = "local";
        public static final String MODE_CLUSTER = "cluster";

        // Kafka default ip address
        public static final String KAFKA_IP_DEFAULT = "localhost";

        // Kafka default port
        public static final Integer KAFKA_PORT_DEFAULT = 9092;

        // Default workers' number
        public static final Integer NUMBER_WORKERS_DEFAULT = 2;

        public static String KAFKA_IP_PORT = KAFKA_IP_DEFAULT + ":" + KAFKA_PORT_DEFAULT;


        // Proprietà "mode"
        public static final String P_MODE = "mode";
        public static String MODE = MODE_LOCAL;

        // Proprietà "kafka-ip"
        public static final String P_KAFKA_IP = "kafka-ip";
        public static String KAFKA_IP = KAFKA_IP_DEFAULT;

        // Proprietà "kafka-port"
        public static final String P_KAFKA_PORT = "kafka-port";
        public static Integer KAFKA_PORT = KAFKA_PORT_DEFAULT;

        // Proprietà "workers"
        public static final String P_NUMBER_OF_WORKERS = "workers";
        public static Integer NUMBER_OF_WORKERS = NUMBER_WORKERS_DEFAULT;

        // Parametri per terza query
        // Proprietà "road-delta"
        // Rappresenta l'errore massimo nell'applicazione che associa
        // una vettuera ad una sequenza di semafori
        public static final String P_ROAD_DELTA = "road-delta";
        public static Double ROAD_DELTA;
        // File (JSON) contenente la descrizione della sequenza di semafori
        public static final String P_SEMAPHORES_SEQUENCES_FILE = "sequences-semaphores-structure";
        public static String SEMAPHORES_SEQUENCES_FILE;

    }

}
