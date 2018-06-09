package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_RANKINGS_OBJECT;

public class GlobalRankingsPublisherBolt extends AbstractKafkaPublisherBolt {

    public GlobalRankingsPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected void doBefore() {

    }

    @Override
    protected String computeStringToPublish(Tuple tuple) {
        Rankings globalRankings = (Rankings) tuple.getValueByField(GLOBAL_RANKINGS_OBJECT);
        return globalRankings.toString();
        // return globalRankings.getJsonFromInstance();
    }

    @Override
    protected void doAfter() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
