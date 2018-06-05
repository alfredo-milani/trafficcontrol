package it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus;

import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.bolts.BaseKafkaPublisherBolt;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class SemaphoreStatusPublisher extends BaseKafkaPublisherBolt {

    @Override
    public void execute(Tuple tuple) {
        try {
            Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
            Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
            Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
            Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
            Long semaphoreTimestampUTC = tuple.getLongByField(SEMAPHORE_TIMESTAMP_UTC);
            String greenLightStatus = tuple.getStringByField(GREEN_LIGHT_STATUS);
            String yellowLightStatus = tuple.getStringByField(YELLOW_LIGHT_STATUS);
            String redLightStatus = tuple.getStringByField(RED_LIGHT_STATUS);

            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put(INTERSECTION_ID, intersectionId);
            objectNode.put(SEMAPHORE_ID, semaphoreId);
            objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
            objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);
            objectNode.put(SEMAPHORE_TIMESTAMP_UTC, semaphoreTimestampUTC);
            objectNode.put(GREEN_LIGHT_STATUS, greenLightStatus);
            objectNode.put(YELLOW_LIGHT_STATUS, yellowLightStatus);
            objectNode.put(RED_LIGHT_STATUS, redLightStatus);

            producer.send(new ProducerRecord<>(SEMAPHORE_STATUS, objectNode.toString()));
        } catch (ClassCastException | IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

}
