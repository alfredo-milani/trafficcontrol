package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_RANKINGS_OBJECT;

public class SequencePublisherBolt extends AbstractKafkaPublisherBolt {

    public SequencePublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected List computeValueToPublish(Tuple tuple) {
        Rankings globalRankings = (Rankings) tuple.getValueByField(GLOBAL_RANKINGS_OBJECT);
        // return new ArrayList<>(Collections.singletonList(globalRankings.toString()));
        // return new ArrayList<>(Collections.singletonList(globalRankings.getJsonStringFromInstance()));
        // TODO getSemaohoresSequence
        return new ArrayList();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
