package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_RANKINGS_OBJECT;

public class RankPublisherBolt extends AbstractKafkaPublisherBolt<String> {

    public RankPublisherBolt(AppConfig appConfig, String topic) {
        super(appConfig, topic);
    }

    @Override
    protected List<String> computeValueToPublish(Tuple tuple) {
        Rankings globalRankings = (Rankings) tuple.getValueByField(GLOBAL_RANKINGS_OBJECT);
        // return new ArrayList<>(Collections.singletonList(globalRankings.toString()));
        return new ArrayList<>(Collections.singletonList(globalRankings.getJsonStringFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
