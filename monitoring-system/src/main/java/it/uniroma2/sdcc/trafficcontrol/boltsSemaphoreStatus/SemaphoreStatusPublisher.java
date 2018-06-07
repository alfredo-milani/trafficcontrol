package it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.StatusSemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_STATUS;

public class SemaphoreStatusPublisher extends AbstractKafkaPublisherBolt {

    public SemaphoreStatusPublisher(String topic) {
        super(topic);
    }

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeStringToPublish(Tuple tuple) {
        StatusSemaphoreSensor sensorStatus = (StatusSemaphoreSensor) tuple.getValueByField(SEMAPHORE_STATUS);

        return sensorStatus.getJsonFromInstance();
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
