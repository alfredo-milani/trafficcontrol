package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class ValidationPublisherBolt extends AbstractKafkaPublisherBolt<String> {

    public ValidationPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected ArrayList<String> computeValueToPublish(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);
        return new ArrayList<>(Collections.singletonList(semaphoreSensor.getJsonStringFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
