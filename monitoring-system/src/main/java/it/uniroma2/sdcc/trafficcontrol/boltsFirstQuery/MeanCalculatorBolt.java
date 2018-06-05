package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.IntersectionMeanSpeedHandler;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.RANKABLE_OBJECT;


public class MeanCalculatorBolt extends BaseRichBolt {

    private HashMap<Long, IntersectionMeanSpeedHandler> handlerHashMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.handlerHashMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Long intersectionId = IntersectionMeanSpeedHandler.getIntersectionIdFrom(tuple);
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getSemaphoreSensorFrom(tuple);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            IntersectionMeanSpeedHandler intersectionFromHashMap = handlerHashMap.putIfAbsent(
                    intersectionId,
                    new IntersectionMeanSpeedHandler(intersectionId)
            );

            if (intersectionFromHashMap != null) {
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);

                if (/* mean has been computed */ true) {
                    // TODO emetti sse sono almeno 2 semafori (con id semaforo pari/dispari)
                    collector.emit(new Values(handlerHashMap.remove(intersectionId)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(RANKABLE_OBJECT));
    }

    public static void main(String[] a) {
        HashMap<Long, IntersectionMeanSpeedHandler> handlerHashMap = new HashMap<>();
        IntersectionMeanSpeedHandler j = new IntersectionMeanSpeedHandler(343L);
        IntersectionMeanSpeedHandler i = new IntersectionMeanSpeedHandler(122L);

        handlerHashMap.put(2L, j);
        // handlerHashMap.putIfAbsent(2L, i);


        System.out.println("TEST: " + i.hashCode());
        System.out.println("TEST: " + j.hashCode());
        System.out.println("TEST: " + handlerHashMap.putIfAbsent(2L, i));
        System.out.println("TEST: " + handlerHashMap.get(2L).hashCode());
        System.out.println("TEST: " + handlerHashMap.size());
    }

}
