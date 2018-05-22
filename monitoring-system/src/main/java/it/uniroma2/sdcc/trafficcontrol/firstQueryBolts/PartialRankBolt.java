package it.uniroma2.sdcc.trafficcontrol.firstQueryBolts;

import it.uniroma2.sdcc.trafficcontrol.utils.IntersectionItem;
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

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.AVERAGE_VEHICLES_SPEED;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.REMOVE;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.UPDATE;
import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.*;


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


        boolean update = false;

        Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
        Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
        Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);
        IntersectionItem item = new IntersectionItem(intersectionId,semaphoreId,semaphoreLatitude,semaphoreLongitude,averageVehiclesSpeed);

 /*       if (!shouldBeInRank) { //streetlight bulb does not need replacement so it must be removed from the ranking if it is present
            int index = ranking.indexOf(item);
            if (index != -1) {
                ranking.remove(item);
                collector.emit(REMOVE, new Values(item));
            }
        } else {
            update = ranking.update(item); //true if item already exists or false if list isn't modified
        }*/

		/* Emit if the local topK is changed */
        if (update) {
            Ranking topK = ranking.getTopK();
            Values values = new Values(topK);
            collector.emit(UPDATE, values);
        }
        collector.ack(tuple);

        System.out.println("PARTIAL BOLT\tupdate: " + update);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(UPDATE, new Fields(PARTIAL_RANK));
        outputFieldsDeclarer.declareStream(REMOVE, new Fields(RANK_ITEM));
    }
}

