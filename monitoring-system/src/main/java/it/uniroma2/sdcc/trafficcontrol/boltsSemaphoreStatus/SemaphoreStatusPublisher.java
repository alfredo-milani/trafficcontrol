package it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.StatusSemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_STATUS;

public class SemaphoreStatusPublisher extends AbstractKafkaPublisherBolt<String> {

    public SemaphoreStatusPublisher(String topic) {
        super(topic);
    }

    @Override
    protected ArrayList<String> computeValueToPublish(Tuple tuple) {
        StatusSemaphoreSensor sensorStatus = (StatusSemaphoreSensor) tuple.getValueByField(SEMAPHORE_STATUS);
        return new ArrayList<>(Collections.singletonList(sensorStatus.getJsonStringFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
