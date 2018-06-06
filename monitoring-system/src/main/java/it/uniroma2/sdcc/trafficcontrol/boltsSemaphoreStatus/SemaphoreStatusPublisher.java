package it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class SemaphoreStatusPublisher extends AbstractKafkaPublisherBolt {

    public SemaphoreStatusPublisher(String topic) {
        super(topic);
    }

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeStringToPublish(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
        Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
        Long semaphoreTimestampUTC = tuple.getLongByField(SEMAPHORE_TIMESTAMP_UTC);
        String greenLightStatus = tuple.getStringByField(GREEN_LIGHT_STATUS);
        String yellowLightStatus = tuple.getStringByField(YELLOW_LIGHT_STATUS);
        String redLightStatus = tuple.getStringByField(RED_LIGHT_STATUS);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(INTERSECTION_ID, intersectionId);
        objectNode.put(SEMAPHORE_ID, semaphoreId);
        objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
        objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);
        objectNode.put(SEMAPHORE_TIMESTAMP_UTC, semaphoreTimestampUTC);
        objectNode.put(GREEN_LIGHT_STATUS, greenLightStatus);
        objectNode.put(YELLOW_LIGHT_STATUS, yellowLightStatus);
        objectNode.put(RED_LIGHT_STATUS, redLightStatus);

        return objectNode.toString();
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
