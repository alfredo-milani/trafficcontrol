package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersectionManager;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
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

    private HashMap<Long, MeanSpeedIntersectionManager> handlerHashMap;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.handlerHashMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Long intersectionId = MeanSpeedIntersectionManager.getIntersectionIdFrom(tuple);
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getSemaphoreSensorFrom(tuple);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            MeanSpeedIntersectionManager intersectionFromHashMap = handlerHashMap.putIfAbsent(
                    intersectionId,
                    new MeanSpeedIntersectionManager(intersectionId)
            );

            if (intersectionFromHashMap != null) {
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);

                if (/* mean has been computed */ true) {
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

}
