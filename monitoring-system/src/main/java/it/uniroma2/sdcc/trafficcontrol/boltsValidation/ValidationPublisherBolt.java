package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.bolts.KafkaBolt;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class ValidationPublisherBolt extends KafkaBolt {

    @Override
    public void execute(Tuple tuple) {
        Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
        Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
        Long semaphoreTimestampUTC = tuple.getLongByField(SEMAPHORE_TIMESTAMP_UTC);
        Short greenLightDuration = tuple.getShortByField(GREEN_LIGHT_DURATION);
        Byte greenLightStatus = tuple.getByteByField(GREEN_LIGHT_STATUS);
        Byte yellowLightStatus = tuple.getByteByField(YELLOW_LIGHT_STATUS);
        Byte redLightStatus = tuple.getByteByField(RED_LIGHT_STATUS);
        Short vehiclesPerSecond = tuple.getShortByField(VEHICLES);
        Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);

        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(INTERSECTION_ID, intersectionId);
        objectNode.put(SEMAPHORE_ID, semaphoreId);
        objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
        objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);
        objectNode.put(SEMAPHORE_TIMESTAMP_UTC, semaphoreTimestampUTC);
        objectNode.put(GREEN_LIGHT_DURATION, greenLightDuration);
        objectNode.put(GREEN_LIGHT_STATUS, greenLightStatus);
        objectNode.put(YELLOW_LIGHT_STATUS, yellowLightStatus);
        objectNode.put(RED_LIGHT_STATUS, redLightStatus);
        objectNode.put(VEHICLES, vehiclesPerSecond);
        objectNode.put(AVERAGE_VEHICLES_SPEED, averageVehiclesSpeed);

        producer.send(new ProducerRecord<>(VALIDATED, objectNode.toString()));

        collector.ack(tuple);
    }

}
