package it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting;

import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.IntersectionHandler;
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

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.VEHICLES;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<Long, IntersectionHandler> handlerHashMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.handlerHashMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {


        try {
            Long intersectionId = IntersectionHandler.getIntersectionIdFrom(tuple);
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getSemaphoreSensorFrom(tuple);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            IntersectionHandler intersectionFromHashMap = handlerHashMap.putIfAbsent(
                    intersectionId,
                    new IntersectionHandler(intersectionId)
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
        }finally {
            collector.ack(tuple);
        }





    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                SEMAPHORE_ID,
                VEHICLES
        ));

    }
}
