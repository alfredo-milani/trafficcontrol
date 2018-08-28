package it.uniroma2.sdcc.influxDBWriter.kafkaReaders;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.java.Log;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.RankingJsonFields.*;

@Log
public class RankingReader extends AbstractKafkaWriter {

    private final String tableName;

    public RankingReader(String dbName, String topicName, String tableName) throws IOException {
        super(dbName, topicName);
        this.tableName = tableName;
    }

    public RankingReader(String dbName, String topicName, Long poolTimeout, String tableName) throws IOException {
        super(dbName, topicName, poolTimeout);
        this.tableName = tableName;
    }

    @Override
    protected BatchPoints computeBatchPoints(JsonNode jsonNode, String dbName) {
        // Creation and definition of a batch containing points for influxDB
        BatchPoints batchPoints = BatchPoints
                .database(dbName)
                .retentionPolicy(RETANTION_POLICY)
                .consistency(InfluxDB.ConsistencyLevel.QUORUM)
                .build();

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
