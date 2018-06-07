package it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting;

import it.uniroma2.sdcc.trafficcontrol.entity.GreenTemporization;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GREEN_TEMPORIZATION_VALUE;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<Long, GreenTemporization> handlerHashMap;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.handlerHashMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            RichSemaphoreSensor richSemaphoreSensor = RichSemaphoreSensor.getInstanceFrom(tuple);
            if (richSemaphoreSensor == null) {
                throw new BadTuple(String.format("Bad tuple: %s", tuple.toString()));
            }

            Long intersectionId = richSemaphoreSensor.getIntersectionId();
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getInstanceFrom(richSemaphoreSensor);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            GreenTemporization intersectionFromHashMap = handlerHashMap.putIfAbsent(
                    intersectionId,
                    new GreenTemporization(intersectionId)
            );

            if (intersectionFromHashMap != null) {
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);
                    //if(semaphoreSensorsEven.size()==2)
                // TODO crea 2 strem: uno per i pari e uno per i dispari e crea i relativi bolt che estendono AbstractKafkaPublisherBolt
                if (intersectionFromHashMap.getSemaphoreSensorsEven().size()==2 ||
                        intersectionFromHashMap.getSemaphoreSensorsOdd().size()==2) {
                    // TODO emetti sse sono almeno 2 semafori (con id semaforo pari/dispari)
                    collector.emit(new Values(handlerHashMap.remove(intersectionId)));
                }
            }

        } catch (ClassCastException | IllegalArgumentException | BadTuple e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(GREEN_TEMPORIZATION_VALUE));

    }

}
