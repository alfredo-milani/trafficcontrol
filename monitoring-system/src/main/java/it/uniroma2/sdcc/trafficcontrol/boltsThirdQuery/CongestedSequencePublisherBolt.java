package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SemaphoresSequence;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SEQUENCE_OBJECT;

public class CongestedSequencePublisherBolt extends AbstractKafkaPublisherBolt<String> {

    public CongestedSequencePublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected List<String> computeValueToPublish(Tuple tuple) {
        SemaphoresSequence semaphoresSequence = (SemaphoresSequence) tuple.getValueByField(SEMAPHORE_SEQUENCE_OBJECT);
        // return new ArrayList<>(Collections.singletonList(semaphoresSequence.toString()));
        return new ArrayList<>(Collections.singletonList(semaphoresSequence.getJsonStringFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
