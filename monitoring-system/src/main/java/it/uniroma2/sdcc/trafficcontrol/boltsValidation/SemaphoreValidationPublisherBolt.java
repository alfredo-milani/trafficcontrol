package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class SemaphoreValidationPublisherBolt extends AbstractKafkaPublisherBolt {

    public SemaphoreValidationPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeStringToPublish(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);
        return semaphoreSensor.getJsonFromInstance();
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
