package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.utils.IntersectionItem;
import it.uniroma2.sdcc.trafficcontrol.utils.Ranking;
import it.uniroma2.sdcc.trafficcontrol.utils.TopKRanking;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class FinalComparator extends BaseRichBolt {

    private OutputCollector collector;
    private TopKRanking ranking;
    private int topK;

    public FinalComparator(){

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        this.collector = collector;
        this.ranking = new TopKRanking(topK);
    }

    @Override
    public void execute(Tuple tuple) {

        boolean updated = false;

        double globalMedian = (double) tuple.getValueByField(VALUE_GLOBAL_MEDIAN);


        if(tuple.getSourceStreamId().equals(UPDATE_GLOBAL_MEDIAN)){
            Ranking intersectiosMedian = (Ranking)tuple.getValueByField(UPDATE_GLOBAL_MEDIAN);
            ArrayList<IntersectionItem> listIntersection = intersectiosMedian.getRanking();
            for(int i=0; i<listIntersection.size(); i++){
                if(listIntersection.get(i).getNumberVehicle()>globalMedian)
                    updated |= ranking.update(listIntersection.get(i));
                System.out.println(ranking.getTopK().getRanking().get(i).getNumberVehicle());
                System.out.println(ranking.getTopK().getRanking().get(i).getIntersectionId());
            }
        }else{
            IntersectionItem intersectionItem = (IntersectionItem) tuple.getValueByField(REMOVE_GLOBAL_MEDIAN_ITEM);
            if (ranking.indexOf(intersectionItem) < topK)
                updated = true;
            ranking.remove(intersectionItem);
        }

        if(updated)

            //printRanking();


       collector.ack(tuple);


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
