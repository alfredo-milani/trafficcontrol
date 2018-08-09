package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MeanIntersectoinSpeedNotReady;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MedianIntersectionNotReady;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEDIAN_VEHICLES_OBJECT;

public class MedianCalculatorBolt extends AbstractWindowedBolt {



    private final Map<Long, MedianIntersection> medianIntersectionQueue;

    public MedianCalculatorBolt(int windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public MedianCalculatorBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        this.medianIntersectionQueue = new HashMap<>();
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        eventsWindow.getExpiredEvents().forEach(t -> medianIntersectionQueue.remove(
                ((RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR)).getIntersectionId()
        ));

        eventsWindow.getNewEvents().forEach(t -> {
            RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR);
            Long intersectionId = richSemaphoreSensor.getIntersectionId();
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getInstanceFrom(richSemaphoreSensor);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            MedianIntersection intersectionFromHashMap = medianIntersectionQueue.putIfAbsent(
                    intersectionId,
                    new MedianIntersection(intersectionId, semaphoreSensor)
            );

            if (intersectionFromHashMap != null) { // Intersezione da aggiornare
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);
                try {
                    intersectionFromHashMap.computeMedianVehiclesIntersection();
                    System.out.println( intersectionId + " MEDIANA-----> "+ medianIntersectionQueue.get(intersectionId).toString() );
                    collector.emit(new Values(intersectionId, medianIntersectionQueue.remove(intersectionId)));
                } catch (MedianIntersectionNotReady e) {
                    // Non sono ancora arrivate tutte le tuple per computare la mediana
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    protected Long getTimestampFrom(Tuple tuple) {
        return ((RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR)).getSemaphoreTimestampUTC();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                INTERSECTION_MEDIAN_VEHICLES_OBJECT
        ));
    }


}
