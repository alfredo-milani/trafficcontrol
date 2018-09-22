package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.RankingJsonFields.*;

public class RankingReader extends AbstractKafkaWriter {

    private final String tableName;

    public RankingReader(String dbName, String topicName, AppConfig appConfig, Long poolTimeout, String tableName) {
        super(dbName, topicName, appConfig, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints attachPointTo(BatchPoints batchPoints, JsonNode jsonNode) {
        JsonNode ranking = jsonNode.get(RANKING);
        Iterator<JsonNode> rankingIterator = ranking.elements();
        while (rankingIterator.hasNext()) {
            JsonNode rankingElement = rankingIterator.next();
            batchPoints.point(
                    Point.measurement(tableName)
                            .time(getAtomicTimestamp(), TimeUnit.MILLISECONDS)
                            .addField(INTERSECTION_ID, rankingElement.get(INTERSECTION_ID).longValue())
                            .addField(MEAN_INTERSECTION_SPEED, rankingElement.get(MEAN_INTERSECTION_SPEED).shortValue())
                            .build()
            );
        }

        return batchPoints;
    }

}
