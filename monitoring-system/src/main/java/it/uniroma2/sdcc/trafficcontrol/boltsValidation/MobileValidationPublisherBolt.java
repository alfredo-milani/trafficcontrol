package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichMobileSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;

public class MobileValidationPublisherBolt extends AbstractKafkaPublisherBolt {

    public MobileValidationPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeStringToPublish(Tuple tuple) {
        RichMobileSensor mobileSensor = (RichMobileSensor) tuple.getValueByField(MOBILE_SENSOR);
        return mobileSensor.getJsonFromInstance();
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
