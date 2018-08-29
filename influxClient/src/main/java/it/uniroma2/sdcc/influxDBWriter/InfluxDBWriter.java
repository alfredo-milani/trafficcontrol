package it.uniroma2.sdcc.influxDBWriter;

import it.uniroma2.sdcc.influxDBWriter.kafkaReaders.*;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;

public class InfluxDBWriter {

    public final static String DB_NAME = "TopologiesResults";

    /*
        Grafana potrebbe non visualizzare dati (No data to show) nel caso in cui la topologia
        considerata non abbia stampato nuovi dati su Kafka perché non ce ne sono.
     */
    public static void main(String[] args)
            throws IOException {
        List<AbstractKafkaWriter> readers = Lists.newArrayList(
                new RankingReader(DB_NAME, RANKING_15_MIN, RANKING_15_MIN),
                new RankingReader(DB_NAME, RANKING_1_H, RANKING_1_H),
                new RankingReader(DB_NAME, RANKING_24_H, RANKING_24_H),
                new CongestIntersectionsWriter(DB_NAME, CONGESTED_INTERSECTIONS_15_MIN, CONGESTED_INTERSECTIONS_15_MIN),
                new CongestIntersectionsWriter(DB_NAME, CONGESTED_INTERSECTIONS_1_H, CONGESTED_INTERSECTIONS_1_H),
                new CongestIntersectionsWriter(DB_NAME, CONGESTED_INTERSECTIONS_24_H, CONGESTED_INTERSECTIONS_24_H),
                new CongestedIntersectionWriter(DB_NAME, CONGESTED_SEQUENCE, CONGESTED_SEQUENCE),
                new GreenTimingWriter(DB_NAME, GREEN_TEMPORIZATION, GREEN_TEMPORIZATION),
                new SemaphoreStatusWriter(DB_NAME, SEMAPHORE_LIGHT_STATUS, SEMAPHORE_LIGHT_STATUS)
        );

        readers.forEach(r -> new Thread(r).start());
    }

}
