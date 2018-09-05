package it.uniroma2.sdcc.trafficcontrol.entity.configuration;

import it.uniroma2.sdcc.trafficcontrol.topologies.*;
import it.uniroma2.sdcc.trafficcontrol.utils.StringUtils;
import lombok.Cleanup;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public class Config extends HashMap<String, Object> {

    // Nome applicazione
    public static final String APPLICATION_NAME = "application-name";
    // Valore di default
    public static final String DEFAULT_APPLICATION_NAME = "Monitoring System";

    // Exit status
    public static final String EXIT_SUCCESS = "exit-success";
    // Valore di default
    public static final int DEFAULT_EXIT_SUCCESS = 0;
    public static final String EXIT_FAILURE = "exit-failure";
    // Valore di default
    public static final int DEFAULT_EXIT_FAILURE = 1;

    public static final String PROPERTIES_LOADED_FROM_FILE = "properties-loaded-from-file";
    public static final boolean DEFAULT_PROPERTIES_LOADED_FROM_FILE = false;

    // Livello debug:
    //     0 = debug disabilitato
    //     ...
    //     5 = debug abilitato; mock end-points per la verifica degli id dei sensori
    // Possibili valori relativi alla chiave DEBUG_LEVEL
    public static final short DEBUG_LEVEL_DISABLED = 0;
    public static final short DEBUG_LEVEL_MOCK_END_POINTS = 5;
    // Debug-level key
    public static final String DEBUG_LEVEL = "debug-level";
    // Valore di default
    public static final short DEFAULT_DEBUG_LEVEL = DEBUG_LEVEL_DISABLED;

    // Nome file proprietà
    public static final String PROPERTIES_FILENAME = "properties-filename";
    // Valore di default
    public static final String DEFAULT_PROPERTIES_FILENAME = "config.properties";

    // Possibili valori relativi alla chiave MODE
    public static final String MODE_LOCAL = "local";
    public static final String MODE_CLUSTER = "cluster";
    // Mode key
    public static final String MODE = "mode";
    // Valore di default
    public static final String DEFAULT_MODE = MODE_LOCAL;

    // Kafka ip key
    public static final String KAFKA_IP = "kafka-ip";
    // Valore di default
    public static final String DEFAULT_KAFKA_IP = "localhost";
    // Kafka port key
    public static final String KAFKA_PORT = "kafka-port";
    // Valore di default
    public static final int DEFAULT_KAFKA_PORT = 9092;
    // Kafka ip:port key
    public static final String KAFKA_IP_PORT = "kafka-ip-port";
    // Valore di default
    public static final String DEFAULT_KAFKA_IP_PORT = DEFAULT_KAFKA_IP + ":" + DEFAULT_KAFKA_PORT;

    // Default workers' number
    public static final String NUMBER_WORKERS = "number-workers";
    public static final int DEFAULT_NUMBER_WORKERS = 2;

    // Parametri per terza query
    // ROAD_DELTA rappresenta l'errore massimo nell'applicazione che associa
    // una vettuera ad una sequenza di semafori
    public static final String ROAD_DELTA = "road-delta";
    // File (JSON) contenente la descrizione della sequenza di semafori
    public static final String SEMAPHORES_SEQUENCES_FILE = "sequences-semaphores-file";

    // Possibili valori
    public static final String TOPOLOGIES_ALL = "all";
    public static final String TOPOLOGY_VALIDATION = ValidationTopology.class.getSimpleName();
    public static final String TOPOLOGY_SEMAPHORE_STATUS = SemaphoreStatusTopology.class.getSimpleName();
    public static final String TOPOLOGY_FIRST = FirstTopology.class.getSimpleName();
    public static final String TOPOLOGY_SECOND = SecondTopology.class.getSimpleName();
    public static final String TOPOLOGY_THIRD = ThirdTopology.class.getSimpleName();
    public static final String TOPOLOGY_GREEN_TIMING = GreenTimingTopology.class.getSimpleName();
    // Topologie da avviare
    public static final String TOPOLOGIES_TO_START = "topologies-to-start";
    // Valore di default
    public static final String DEFAULT_TOPOLOGIES_TO_START = TOPOLOGIES_ALL;

    // Endpoints per i sensori semaforici e mobili
    // NOTA: gli endpoints devono avere il seguente formato: [protocollo]://[ipAddress]:[port]/[path]/[%d]
    // dove "%d" deve essere sostituito con l'id del sensore che si sta cercando
    public static final String SEMAPHORES_SENSORS_ENDPOINT = "semaphores-sensors-endpoint";
    public static final String MOBILE_SENSORS_ENDPOINT = "mobile-sensors-endpoint";



    private static class SingletonContainer {
        private final static Config instance = new Config();
    }

    protected Config() {
        put(APPLICATION_NAME, DEFAULT_APPLICATION_NAME);
        put(EXIT_SUCCESS, DEFAULT_EXIT_SUCCESS);
        put(EXIT_FAILURE, DEFAULT_EXIT_FAILURE);
        put(PROPERTIES_FILENAME, DEFAULT_PROPERTIES_FILENAME);
        put(PROPERTIES_LOADED_FROM_FILE, DEFAULT_PROPERTIES_LOADED_FROM_FILE);
        put(DEBUG_LEVEL, DEFAULT_DEBUG_LEVEL);

        put(MODE, DEFAULT_MODE);
        put(KAFKA_IP, DEFAULT_KAFKA_IP);
        put(KAFKA_PORT, DEFAULT_KAFKA_PORT);
        put(KAFKA_IP_PORT, DEFAULT_KAFKA_IP_PORT);

        put(NUMBER_WORKERS, DEFAULT_NUMBER_WORKERS);

        put(TOPOLOGIES_TO_START, Lists.newArrayList(DEFAULT_TOPOLOGIES_TO_START));
    }

    public static Config getInstance() {
        return SingletonContainer.instance;
    }

    public static Config getInstanceAndLoad() throws IOException {
        if (!(boolean) SingletonContainer.instance.get(PROPERTIES_LOADED_FROM_FILE)) {
            SingletonContainer.instance.load();
        }
        return SingletonContainer.instance;
    }

    public void load()
            throws IOException {
        load((String) get(PROPERTIES_FILENAME));
    }

    public void load(String configurationFile)
            throws IOException {
        if (configurationFile != null) {
            put(PROPERTIES_FILENAME, configurationFile);
        }

        @Cleanup InputStream input = Config.class
                .getClassLoader()
                .getResourceAsStream((String) get(PROPERTIES_FILENAME));

        if (input == null) {
            throw new IOException("Sorry, unable to find " + get(PROPERTIES_FILENAME));
        }

        Properties properties = new Properties();
        properties.load(input);

        Object tmp;
        if ((tmp = properties.getProperty(MODE)) != null) {
            put(MODE, tmp.toString());
        }
        if ((tmp = properties.getProperty(KAFKA_IP)) != null) {
            put(KAFKA_IP, tmp);
        }
        if ((tmp = properties.getProperty(KAFKA_PORT)) != null) {
            put(KAFKA_PORT, Integer.valueOf(tmp.toString()));
        }
        if (!String.format("%s:%d", get(KAFKA_IP), (int) get(KAFKA_PORT)).equals(get(KAFKA_IP_PORT))) {
            put(KAFKA_IP_PORT, get(KAFKA_IP) + ":" + get(KAFKA_PORT));
        }
        if ((tmp = properties.getProperty(NUMBER_WORKERS)) != null) {
            put(NUMBER_WORKERS, Integer.valueOf(tmp.toString()));
        }
        if ((tmp = properties.getProperty(ROAD_DELTA)) != null) {
            put(ROAD_DELTA, Double.valueOf(tmp.toString()));
        }
        if ((tmp = properties.getProperty(SEMAPHORES_SEQUENCES_FILE)) != null) {
            put(SEMAPHORES_SEQUENCES_FILE, tmp.toString());
        }
        if ((tmp = properties.getProperty(TOPOLOGIES_TO_START)) != null) {
            put(TOPOLOGIES_TO_START, StringUtils.fromStringToList(tmp.toString()));
        }
        if ((tmp = properties.getProperty(SEMAPHORES_SENSORS_ENDPOINT)) != null) {
            put(SEMAPHORES_SENSORS_ENDPOINT, tmp.toString());
        }
        if ((tmp = properties.getProperty(MOBILE_SENSORS_ENDPOINT)) != null) {
            put(MOBILE_SENSORS_ENDPOINT, tmp.toString());
        }

        put(PROPERTIES_LOADED_FROM_FILE, !DEFAULT_PROPERTIES_LOADED_FROM_FILE);
        if ((tmp = properties.getProperty(DEBUG_LEVEL)) != null) {
            put(DEBUG_LEVEL, Short.valueOf(tmp.toString()));
        }
    }

    public boolean hasBeenLoaded() {
        return (boolean) get(PROPERTIES_LOADED_FROM_FILE);
    }

    public void loadIfHasNotAlreadyBeenLoaded() throws IOException {
        if (!(boolean) get(PROPERTIES_LOADED_FROM_FILE)) {
            load();
        }
    }

    public void setConfigurationFile(String configurationFile) {
        put(DEFAULT_PROPERTIES_FILENAME, configurationFile);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("\n########################################\n");
        stringBuilder.append("### \t\tApplication's properties\t\t\n");
        keySet().forEach(k -> stringBuilder
                .append("# \t")
                .append(k)
                .append(":\t")
                .append(get(k))
                .append("\n"));
        stringBuilder.append("########################################\n");
        return stringBuilder.toString();
    }

}
