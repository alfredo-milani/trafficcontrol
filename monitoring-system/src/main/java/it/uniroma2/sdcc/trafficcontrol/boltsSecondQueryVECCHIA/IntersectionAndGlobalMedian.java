package it.uniroma2.sdcc.trafficcontrol.boltsSecondQueryVECCHIA;

import com.tdunning.math.stats.TDigest;
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

import java.util.ArrayList;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class IntersectionAndGlobalMedian extends BaseRichBolt {
    private OutputCollector collector;
    private double compression = 100;
    private double quantile = 0.5; //mediana
    //lo uso solo per aggiornare i valori
    private TopKRanking globalList;
    //private ArrayList<TopKRanking> intersectionList;

    private int numberOfSensor;


    public IntersectionAndGlobalMedian(){}


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        //imposto di default topK=50 poi lo aggiorno in base al numero di semafori funzionanti
        this.globalList= new TopKRanking(numberOfSensor);
       /* this.intersectionList = new ArrayList<TopKRanking>() {{
        for(int i = 0; i<50;i++)
                intersectionList.add( new TopKRanking(4)); //4 = numero di semafori per intersezione

        }};*/

    }

    @Override
    public void execute(Tuple tuple) {

        boolean update = false;

        Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Boolean semaphoreStatus = tuple.getBooleanByField(SEMAPHORE_STATUS);
        Short numeberVehicle = tuple.getShortByField(VEHICLES);
        IntersectionItem item = new IntersectionItem(intersectionId,semaphoreId,numeberVehicle);


       if(!semaphoreStatus){
            int index = globalList.indexOf(item);
            if (index != -1) {
                globalList.remove(item);
                collector.emit(REMOVE_GLOBAL_MEDIAN_ITEM, new Values(item));
            }
        }else {
           int index = globalList.indexOf(item);
           if (index != -1) {
               //TODO fare bene questa parte cosi fa la mediana solo tra il valore precedente e il nuovo
               TDigest tDigestIntesection = TDigest.createAvlTreeDigest(compression);
               ArrayList<IntersectionItem> listIntersectionNotUpdate = globalList.getTopK().getRanking();
               tDigestIntesection.add(listIntersectionNotUpdate.get(index).getNumberVehicle());
               tDigestIntesection.add(item.getNumberVehicle());
               double intersectionMedian = tDigestIntesection.quantile(quantile);
               item.setNumberVehicle((short) intersectionMedian);
           }

           update = globalList.update(item);
       }
        if (update) {
            Ranking topK = globalList.getTopK();
            Values values = new Values(topK);
            collector.emit(UPDATE_GLOBAL_MEDIAN, values);

        }

        TDigest tDigestGlobal = TDigest.createAvlTreeDigest(compression);

       ArrayList<IntersectionItem> effectiveGlobalList = globalList.returnArray();
       for (int i=0; i< effectiveGlobalList.size();i++) {
           tDigestGlobal.add(effectiveGlobalList.get(i).getNumberVehicle());
       }
        double globalMedian = tDigestGlobal.quantile(quantile);
       collector.emit(VALUE_GLOBAL_MEDIAN, new Values(globalMedian));

        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

        declarer.declareStream(UPDATE_GLOBAL_MEDIAN, new Fields(UPDATE_GLOBAL_MEDIAN));
        declarer.declareStream(REMOVE_GLOBAL_MEDIAN_ITEM, new Fields(REMOVE_GLOBAL_MEDIAN_ITEM));
        declarer.declareStream(VALUE_GLOBAL_MEDIAN, new Fields(VALUE_GLOBAL_MEDIAN));

    }
}
