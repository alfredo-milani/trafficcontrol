package it.uniroma2.sdcc.trafficcontrol.firstquerybolts;

import it.uniroma2.sdcc.trafficcontrol.constants.StormParams;
import it.uniroma2.sdcc.trafficcontrol.constants.TupleFields;
import it.uniroma2.sdcc.trafficcontrol.utils.RankItem;
import it.uniroma2.sdcc.trafficcontrol.utils.Ranking;
import it.uniroma2.sdcc.trafficcontrol.utils.TopKRanking;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class PartialRankBolt extends BaseRichBolt {
    /**
     * Determination of the partial ranking of streetlamps that need replacing.
     */
    private OutputCollector collector;
    private TopKRanking ranking;
    private int topK;


    public PartialRankBolt(int topk) {
        this.topK = topk;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.ranking = new TopKRanking(topK);
    }

    @Override
    public void execute(Tuple tuple) {

        Integer id = tuple.getIntegerByField(TupleFields.ID);
        String city = tuple.getStringByField(TupleFields.CITY);
        String address = tuple.getStringByField(TupleFields.ADDRESS);
        Integer km = tuple.getIntegerByField(TupleFields.KM);
        String model = tuple.getStringByField(TupleFields.BULB_MODEL);
        Long installationTimestamp = tuple.getLongByField(TupleFields.INSTALLATION_TIMESTAMP);
        Long meanExpirationTime = tuple.getLongByField(TupleFields.MEAN_EXPIRATION_TIME);
        Boolean shouldBeInRank = tuple.getBooleanByField(TupleFields.SHOULD_BE_IN_RANK);

        boolean update = false;

        RankItem item = new RankItem(id, city, address, km, model, installationTimestamp, meanExpirationTime);

        if (!shouldBeInRank) { //streetlight bulb does not need replacement so it must be removed from the ranking if it is present
            int index = ranking.indexOf(item);
            if (index != -1) {
                ranking.remove(item);
                collector.emit(StormParams.REMOVE, new Values(item));
            }
        } else {
            update = ranking.update(item); //true if item already exists or false if list isn't modified
        }

		/* Emit if the local topK is changed */
        if (update) {
            Ranking topK = ranking.getTopK();
            Values values = new Values(topK);
            collector.emit(StormParams.UPDATE, values);
        }
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StormParams.UPDATE, new Fields(TupleFields.PARTIAL_RANK));
        outputFieldsDeclarer.declareStream(StormParams.REMOVE, new Fields(TupleFields.RANK_ITEM));
    }
}

