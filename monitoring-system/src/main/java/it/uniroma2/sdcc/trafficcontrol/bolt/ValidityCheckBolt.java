package it.uniroma2.sdcc.trafficcontrol.bolt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class ValidityCheckBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);

        try {
            String rawTuple = tuple.getStringByField(KAFKA_RAW_TUPLE);
            JsonNode jsonNode = mapper.readTree(rawTuple);

            // Verifica correttezza valori tupla
            Long intersectionId = jsonNode.get(INTERSECTION_ID).asLong();
            Long semaphoreId = jsonNode.get(SEMAPHORE_ID).asLong();
            Double semaphoreLatitude = jsonNode.get(SEMAPHORE_LATITUDE).asDouble();
            Double semaphoreLongitude = jsonNode.get(SEMAPHORE_LONGITUDE).asDouble();
            Long semaphoreTimestampUTC = jsonNode.get(SEMAPHORE_TIMESTAMP_UTC).asLong();
            Short greenLightDuration = jsonNode.get(GREEN_LIGHT_DURATION).shortValue();
            Byte greenLightStatus = (byte) jsonNode.get(GREEN_LIGHT_STATUS).asInt();
            Byte yellowLightStatus = (byte) jsonNode.get(YELLOW_LIGHT_STATUS).asInt();
            Byte redLightStatus = (byte) jsonNode.get(RED_LIGHT_STATUS).asInt();
            Short vehiclesPerSecond = jsonNode.get(VEHICLES_PER_SECOND).shortValue();
            Short averageVehiclesSpeed = jsonNode.get(AVERAGE_VEHICLES_SPEED).shortValue();

            Values values = new Values(
                    intersectionId,
                    semaphoreId,
                    semaphoreLatitude,
                    semaphoreLongitude,
                    semaphoreTimestampUTC,
                    greenLightDuration,
                    greenLightStatus,
                    yellowLightStatus,
                    redLightStatus,
                    vehiclesPerSecond,
                    averageVehiclesSpeed
            );

            collector.emit(values);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                SEMAPHORE_ID,
                SEMAPHORE_LATITUDE,
                SEMAPHORE_LONGITUDE,
                SEMAPHORE_TIMESTAMP_UTC,
                GREEN_LIGHT_DURATION,
                GREEN_LIGHT_STATUS,
                YELLOW_LIGHT_STATUS,
                RED_LIGHT_STATUS,
                VEHICLES_PER_SECOND,
                AVERAGE_VEHICLES_SPEED
        ));
    }

}