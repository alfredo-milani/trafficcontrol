package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;


public class MeanCalculatorBolt extends BaseRichBolt {

    private Map<Long, MeanSpeedIntersection> handlerHashMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.handlerHashMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);

            Long intersectionId = richSemaphoreSensor.getIntersectionId();
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getInstanceFrom(richSemaphoreSensor);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            MeanSpeedIntersection intersectionFromHashMap = handlerHashMap.putIfAbsent(
                    intersectionId,
                    new MeanSpeedIntersection(intersectionId)
            );

            // TODO mettere finesrta temporale anche qui e nella add() controllare se aggiungere all'intersezione o se
            // crearne una nuova perché quella è scaduta
            if (intersectionFromHashMap != null) { // Intersezione da aggiornare
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);
                if (intersectionFromHashMap.isListReadyForComputation()) { // Controllo se sono arrivate tutte le tuple per computare la media
                    intersectionFromHashMap.computeIntersectionMeanSpeed();
                    if (intersectionFromHashMap.isMeanComputed()) {
                        collector.emit(new Values(intersectionId, handlerHashMap.remove(intersectionId)));
                    }
                }
            }
        } catch (ClassCastException | IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                INTERSECTION_MEAN_SPEED_OBJECT
        ));
    }

}
