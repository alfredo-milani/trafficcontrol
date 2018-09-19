package it.uniroma2.sdcc.trafficcontrol.entity.configuration;

import it.uniroma2.sdcc.trafficcontrol.topologies.*;
import it.uniroma2.sdcc.trafficcontrol.utils.StringUtils;
import lombok.Cleanup;
import lombok.NonNull;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    // Parametri per terza query
    // ROAD_DELTA rappresenta l'errore massimo nell'applicazione che associa
    // una vettuera ad una sequenza di semafori
    public static final String ROAD_DELTA = "road-delta";
    // Valore di default
    public static final double DEFAULT_ROAD_DELTA = 0.00003;
    // File (JSON) contenente la descrizione della sequenza di semafori
    public static final String SEMAPHORES_SEQUENCES_FILE = "sequences-semaphores-file";
    // Valore di default
    public static final String DEFAULT_SEMAPHORES_SEQUENCES_FILE = "SemaphoresSequencesStructure.json";

    // Possibili valori
    public static final String TOPOLOGY_VALIDATION = ValidationTopology.class.getSimpleName();
    public static final String TOPOLOGY_SEMAPHORE_STATUS = SemaphoreStatusTopology.class.getSimpleName();
    public static final String TOPOLOGY_FIRST = FirstTopology.class.getSimpleName();
    public static final String TOPOLOGY_SECOND = SecondTopology.class.getSimpleName();
    public static final String TOPOLOGY_THIRD = ThirdTopology.class.getSimpleName();
    public static final String TOPOLOGY_GREEN_TIMING = GreenTimingTopology.class.getSimpleName();
    // Topologie da avviare
    public static final String TOPOLOGIES = "topologies";
    // Valore di default
    public static final List<String> DEFAULT_TOPOLOGIES = Lists.newArrayList(
            TOPOLOGY_VALIDATION,
            TOPOLOGY_SEMAPHORE_STATUS,
            TOPOLOGY_FIRST,
            TOPOLOGY_SECOND,
            TOPOLOGY_THIRD,
            TOPOLOGY_GREEN_TIMING
    );

    // Default workers' number
    public static final String WORKERS_NUMBER = "workers-number";
    public static final int DEFAULT_WORKERS_NUMBER = 2;

    // Endpoints per i sensori semaforici e mobili
    // NOTA: gli endpoints devono avere il seguente formato: [protocollo]://[ipAddress]:[port]/[path]/[%d]
    // dove "%d" deve essere sostituito con l'id del sensore che si sta cercando
    public static final String SEMAPHORES_SENSORS_ENDPOINT = "semaphores-sensors-endpoint";
    public static final String MOBILE_SENSORS_ENDPOINT = "mobile-sensors-endpoint";



    // Singleton con inizializzazione statica è thread-safe solo in caso ci sia una sola JVM
    // e quindi un solo Class Loader
    private static class SingletonContainer {
        private final static Config INSTANCE = new Config();
    }

    protected Config() {
        // Valori di default per l'applicazione
        put(APPLICATION_NAME, DEFAULT_APPLICATION_NAME);
        put(EXIT_SUCCESS, DEFAULT_EXIT_SUCCESS);
        put(EXIT_FAILURE, DEFAULT_EXIT_FAILURE);
        put(PROPERTIES_FILENAME, DEFAULT_PROPERTIES_FILENAME);
        put(PROPERTIES_LOADED_FROM_FILE, DEFAULT_PROPERTIES_LOADED_FROM_FILE);
        put(DEBUG_LEVEL, DEFAULT_DEBUG_LEVEL);

        // Valori di default per kafka
        put(MODE, DEFAULT_MODE);
        put(KAFKA_IP, DEFAULT_KAFKA_IP);
        put(KAFKA_PORT, DEFAULT_KAFKA_PORT);
        put(KAFKA_IP_PORT, DEFAULT_KAFKA_IP_PORT);

        // Valori di default per la terza topologia
        put(ROAD_DELTA, DEFAULT_ROAD_DELTA);
        put(SEMAPHORES_SEQUENCES_FILE, DEFAULT_SEMAPHORES_SEQUENCES_FILE);

        // VAlori di default per le topologie
        put(TOPOLOGIES, DEFAULT_TOPOLOGIES);
        put(WORKERS_NUMBER, DEFAULT_WORKERS_NUMBER);
    }

    public static Config getInstance() {
        return SingletonContainer.INSTANCE;
    }

    public static Config getInstanceAndLoad()
            throws IOException {
        SingletonContainer.INSTANCE.loadIfHasNotAlreadyBeenLoaded();
        return SingletonContainer.INSTANCE;
    }

    public void load()
            throws IOException {
        load(Config.class
                .getClassLoader()
                .getResourceAsStream(getConfigurationFilename())
        );
    }

    public void load(@NonNull String configurationFilename)
            throws IOException{
        setConfigurationFilename(configurationFilename);
        load(new FileInputStream(configurationFilename));
    }

    public void load(@NonNull InputStream confInputStream)
            throws IOException {
        @Cleanup InputStream configInputStream = confInputStream;
        @Cleanup BufferedInputStream configBufferedInputStream = new BufferedInputStream(configInputStream);
        Properties properties = new Properties();
        properties.load(configBufferedInputStream);

        String tmp;
        if ((tmp = properties.getProperty(MODE)) != null) {
            put(MODE, tmp);
        }
        if ((tmp = properties.getProperty(KAFKA_IP)) != null) {
            put(KAFKA_IP, tmp);
        }
        if ((tmp = properties.getProperty(KAFKA_PORT)) != null) {
            put(KAFKA_PORT, Integer.valueOf(tmp));
        }
        if (!String.format("%s:%d", get(KAFKA_IP), (int) get(KAFKA_PORT)).equals(get(KAFKA_IP_PORT))) {
            put(KAFKA_IP_PORT, get(KAFKA_IP) + ":" + get(KAFKA_PORT));
        }
        if ((tmp = properties.getProperty(ROAD_DELTA)) != null) {
            put(ROAD_DELTA, Double.valueOf(tmp));
        }
        if ((tmp = properties.getProperty(SEMAPHORES_SEQUENCES_FILE)) != null) {
            put(SEMAPHORES_SEQUENCES_FILE, tmp);
        }
        if ((tmp = properties.getProperty(TOPOLOGIES)) != null) {
            put(TOPOLOGIES, StringUtils.fromStringToList(tmp));
        }
        if ((tmp = properties.getProperty(WORKERS_NUMBER)) != null) {
            put(WORKERS_NUMBER, Integer.valueOf(tmp));
        }
        if ((tmp = properties.getProperty(SEMAPHORES_SENSORS_ENDPOINT)) != null) {
            put(SEMAPHORES_SENSORS_ENDPOINT, tmp);
        }
        if ((tmp = properties.getProperty(MOBILE_SENSORS_ENDPOINT)) != null) {
            put(MOBILE_SENSORS_ENDPOINT, tmp);
        }

        put(PROPERTIES_LOADED_FROM_FILE, !DEFAULT_PROPERTIES_LOADED_FROM_FILE);
        if ((tmp = properties.getProperty(DEBUG_LEVEL)) != null) {
            put(DEBUG_LEVEL, Short.valueOf(tmp));
        }
    }

    public void loadIfHasNotAlreadyBeenLoaded()
            throws IOException {
        if (!hasBeenLoaded()) {
            load();
        }
    }

    public void setConfigurationFilename(String configurationFile) {
        put(PROPERTIES_FILENAME, configurationFile);
    }

    public String getApplicationName() {
        return (String) get(APPLICATION_NAME);
    }

    public int getExitSuccess() {
        return (int) get(EXIT_SUCCESS);
    }

    public int getExitFailure() {
        return (int) get(EXIT_FAILURE);
    }

    public boolean hasBeenLoaded() {
        return (boolean) get(PROPERTIES_LOADED_FROM_FILE);
    }

    public short getDebugLevel() {
        return (short) get(DEBUG_LEVEL);
    }

    public String getConfigurationFilename() {
        return (String) get(PROPERTIES_FILENAME);
    }

    public String getMode() {
        return (String) get(MODE);
    }

    public String getKafkaIp() {
        return (String) get(KAFKA_IP);
    }

    public int getKafkaPort() {
        return (int) get(KAFKA_PORT);
    }

    public String getKafkaIpPort() {
        return (String) get(KAFKA_IP_PORT);
    }

    public int getNumberWorkers() {
        return (int) get(WORKERS_NUMBER);
    }

    public Double getRoadDelta() {
        return (Double) get(ROAD_DELTA);
    }

    /**
     * Utilizza il file di configurazione per ottenere il path del file contenete la sequenza
     * dei semafori.
     * Se non viene specificato alcun valore nel file di configurazione allora verrà
     * utilizzato il file nella directory resources chiamato {@link Config#DEFAULT_SEMAPHORES_SEQUENCES_FILE}
     *
     * NOTA: è lasciato al chiamante il compito di chiudere l'InputStream
     *
     * @return Input stream bufferizzato del file di configurazinoe della sequenza dei semafori
     */
    public InputStream getSemaphoresSequencesInputStream()
            throws FileNotFoundException {
        String fileToStream = (String) get(SEMAPHORES_SEQUENCES_FILE);
        if (fileToStream.equals(DEFAULT_SEMAPHORES_SEQUENCES_FILE)) {
            // Nessun valore specificato
            // Verrà utilizzato il valore dei default nella directory resources
            return Config.class.getClassLoader().getResourceAsStream(fileToStream);
        } else {
            // Valore custom specificato per il file di configurazione
            // della struttura della sequenza dei semafori
            return new FileInputStream(fileToStream);
        }
    }

    public String getSemaphoresSequencesFilename() {
        return (String) get(SEMAPHORES_SEQUENCES_FILE);
    }

    @SuppressWarnings("unchecked")
    public List<Topology> getTopologies() {
        List<Topology> topologies = new ArrayList<>();

        List<String> topologiesToStart = (List<String>) get(TOPOLOGIES);
        topologiesToStart.forEach(s -> {
            if (s.equals(TOPOLOGY_VALIDATION))              topologies.add(new ValidationTopology());
            else if (s.equals(TOPOLOGY_SEMAPHORE_STATUS))   topologies.add(new SemaphoreStatusTopology());
            else if (s.equals(TOPOLOGY_FIRST))              topologies.add(new FirstTopology());
            else if (s.equals(TOPOLOGY_SECOND))             topologies.add(new SecondTopology());
            else if (s.equals(TOPOLOGY_THIRD))              topologies.add(new ThirdTopology());
            else if (s.equals(TOPOLOGY_GREEN_TIMING))       topologies.add(new GreenTimingTopology());
            else System.err.println(String.format("Topologia sconosciuta: \"%s\"", s));
        });

        return topologies;
    }

    public String getSemaphoresSensorsEndpoint() {
        return (String) get(SEMAPHORES_SENSORS_ENDPOINT);
    }

    public String getMobileSensorsEndpoint() {
        return (String) get(MOBILE_SENSORS_ENDPOINT);
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
