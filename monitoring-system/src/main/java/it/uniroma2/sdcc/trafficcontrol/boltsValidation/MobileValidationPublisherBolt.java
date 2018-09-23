package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;

public class MobileValidationPublisherBolt extends AbstractKafkaPublisherBolt<String> {

    public MobileValidationPublisherBolt(AppConfig appConfig, String topic) {
        super(appConfig, topic);
    }

    @Override
    protected ArrayList<String> computeValueToPublish(Tuple tuple) {
        RichMobileSensor mobileSensor = (RichMobileSensor) tuple.getValueByField(MOBILE_SENSOR);
        return new ArrayList<>(Collections.singletonList(mobileSensor.getJsonStringFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
