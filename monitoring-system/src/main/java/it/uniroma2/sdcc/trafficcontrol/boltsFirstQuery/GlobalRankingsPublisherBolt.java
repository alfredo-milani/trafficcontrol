package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_RANKINGS_OBJECT;

public class GlobalRankingsPublisherBolt extends AbstractKafkaPublisherBolt<String> {

    public GlobalRankingsPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected ArrayList<String> computeStringToPublish(Tuple tuple) {
        Rankings globalRankings = (Rankings) tuple.getValueByField(GLOBAL_RANKINGS_OBJECT);
        return new ArrayList<>(Collections.singletonList(globalRankings.toString()));
        // return new ArrayList<>(Collections.singletonList(globalRankings.getJsonFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
