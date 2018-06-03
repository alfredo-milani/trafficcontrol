package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.utils.IntersectionItem;
import it.uniroma2.sdcc.trafficcontrol.utils.Ranking;
import it.uniroma2.sdcc.trafficcontrol.utils.TopKRanking;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.UPDATE_PARTIAL;

public class PartialRankWindowedBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private int d;
    private final int topK;
    private TopKRanking ranking;

    public PartialRankWindowedBolt(int topK) {
        this.topK = topK;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.ranking = new TopKRanking(topK);
        this.d = ThreadLocalRandom.current().nextInt(0, 100);
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        // System.out.println("--- INIZIO FINESTRA ID" + d + "----" + System.currentTimeMillis());
        List<Tuple> tuplesInWindow = tupleWindow.getNew();
        // tupleWindow.get().forEach(s -> System.out.println(String.format("P_BOLT: %d\tTUPLE: %d", d, s.getLongByField(SEMAPHORE_ID))));
        // tupleWindow.getNew().forEach(s -> System.out.println(String.format("P_BOLT: %d\tTUPLE NEW: %d", d, s.getLongByField(SEMAPHORE_ID))));
        // tupleWindow.getExpired().forEach(s -> System.out.println(String.format("P_BOLT: %d\tTUPLE EXP: %d", d, s.getLongByField(SEMAPHORE_ID))));

        boolean update = false;
        for (Tuple tuple : tuplesInWindow) {
            Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
            Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
            Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
            Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
            Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);
            IntersectionItem item = new IntersectionItem(intersectionId, semaphoreId, semaphoreLatitude, semaphoreLongitude, averageVehiclesSpeed);

            // System.out.println(String.format("BOLT: %d\tINT ID: %d\n\n", d, intersectionId));

            /*if (!semaphoreStatus) {
                int index = ranking.indexOf(item);
                if (index != -1) {
                    ranking.remove(item);
                    collector.emit(REMOVE, new Values(item));
                }
            } else {
                update = ranking.update(item);
            }*/

            update = ranking.update(item);

            // superfluo perché le tuple expired sono ackate automaticamente
            collector.ack(tuple);

            // System.out.println(String.format("INT: %d\tSEM: %d", intersectionId, semaphoreId));
        }

        // TODO vedi se lasciare sempahore status
        /* Emit if the local topK is changed */
        if (update) {
            Ranking topK = ranking.getTopK();
            Values values = new Values(topK);
            collector.emit(values);
        }
        // System.out.println("--- FINE FINESTRA ID" + d + "---" + System.currentTimeMillis() + "\n");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(UPDATE_PARTIAL));
    }


    // TODO nel calcolo della media rimuovi solo se è stato  ricevuto l'ack (metodo onack)

    // TODO in un incrocio a X è sicuro che il verde è a 2 a 2?
    // se è a 2 a 2 devo aspetta quelli pari/dispari e fa la media su 2 elementi senno viene sballata

    // TODO vedi se mettere statu semaphore in query separata (vedi se spout kafka prende cmq le tuple o se
    // vengono scartate perché lette da spout precedente)

    // TODO i partial bolt devo sempre emettere tuple? Perché, nel caso in cui la posizione nella classifica
    // locale non cambia, ma cambia solo il valore della media, nella classifica globale potrebbe cambiare la posizione
    // in classifica.
    // Inoltre, se nella classifica globale cambia solo il valore ma la posizione rimane la stessa, si deve
    // produrre output?

    // TODO anche qui i valori expired devono essere eliminati dalla classifica locale (quindi anche questo bolt
    // deve avere finesrtre temporali) altrimenti valori vecchi (scaduti dopo i 15 minuti) saranno inviati al
    // global bolt anche se nella sua finestra temporale sono usciti e quindi non andrebbero considerati

}
