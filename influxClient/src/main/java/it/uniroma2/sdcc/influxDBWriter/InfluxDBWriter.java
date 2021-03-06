package it.uniroma2.sdcc.influxDBWriter;

import it.uniroma2.sdcc.influxDBWriter.kafkaReaders.*;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;

public class InfluxDBWriter {

    public final static String DB_NAME = "TopologiesResults";
    public final static long CUSTOM_POOL_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
    public final static AppConfig APP_CONFIG = AppConfig.getInstance();

    /*
        1. Grafana potrebbe non visualizzare dati (No data to show) nel caso in cui
        la topologia considerata non abbia stampato nuovi dati su Kafka perché non ce
        ne sono di nuovi e Grafana è stato impostato per l'acquisizione di dati degli ultimi 5 secondi.

        2. Dove sono previste più finestre temporali (prima e seconda query) potrebbe accadere che queste
        non siano perfettamente allineate. Questo fenomeno si verifica in quanto i bolts gestori delle
        rispettive finestre temporali non sono avviati dalla JVM precisamente negli stessi istanti.
        Questo fenomeno è reso ancor più evidente quando si utilizza una database per la fase di autenticazione
        dei sensori in quanto si introduce un ulteriore overhead tra l'arrivo di una tupla nel sistema e la sua computazione.
     */
    public static void main(String[] args)
            throws IOException {
        // Controllo se è stato passato un file di configurazione per
        // l'applicazione da linea di comando
        if (args.length != 0) APP_CONFIG.load(args[0]);
        else APP_CONFIG.load();

        List<AbstractKafkaWriter> readers = Lists.newArrayList(
                new RankingReader(DB_NAME, RANKING_15_MIN, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, RANKING_15_MIN),
                new RankingReader(DB_NAME, RANKING_1_H, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, RANKING_1_H),
                new RankingReader(DB_NAME, RANKING_24_H, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, RANKING_24_H),
                new CongestIntersectionsWriter(DB_NAME, CONGESTED_INTERSECTIONS_15_MIN, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, CONGESTED_INTERSECTIONS_15_MIN),
                new CongestIntersectionsWriter(DB_NAME, CONGESTED_INTERSECTIONS_1_H, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, CONGESTED_INTERSECTIONS_1_H),
                new CongestIntersectionsWriter(DB_NAME, CONGESTED_INTERSECTIONS_24_H, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, CONGESTED_INTERSECTIONS_24_H),
                new CongestedIntersectionWriter(DB_NAME, CONGESTED_SEQUENCE, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, CONGESTED_SEQUENCE),
                new GreenTimingWriter(DB_NAME, GREEN_TEMPORIZATION, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, GREEN_TEMPORIZATION),
                new SemaphoreStatusWriter(DB_NAME, SEMAPHORE_LIGHT_STATUS, APP_CONFIG, CUSTOM_POOL_TIMEOUT_MILLIS, SEMAPHORE_LIGHT_STATUS)
        );

        readers.forEach(r -> new Thread(r).start());
    }

}
