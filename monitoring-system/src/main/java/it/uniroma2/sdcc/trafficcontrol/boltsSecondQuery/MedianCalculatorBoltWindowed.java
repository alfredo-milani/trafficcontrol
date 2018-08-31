package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.timeWindow.ITimeWindow;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MedianIntersectionNotReady;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_NUMBER_TO_COMPUTE_MEDIAN;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEDIAN_VEHICLES_OBJECT;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.MEDIAN_INTERSECTION_STREAM;

public class MedianCalculatorBoltWindowed extends AbstractWindowedBolt {

    private final Map<Long, MedianIntersection> medianIntersectionQueue;

    public MedianCalculatorBoltWindowed(int windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public MedianCalculatorBoltWindowed(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        this.medianIntersectionQueue = new HashMap<>();
    }

    @Override
    protected void onTick(OutputCollector collector, ITimeWindow<Tuple> eventsWindow) {
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
                    intersectionFromHashMap.computeMedianVehiclesIntersection(SEMAPHORE_NUMBER_TO_COMPUTE_MEDIAN);
                    collector.emit(MEDIAN_INTERSECTION_STREAM, new Values(medianIntersectionQueue.remove(intersectionId)));
                } catch (MedianIntersectionNotReady e) {
                    // Non sono ancora arrivate tutte le tuple per computare la mediana
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
        declarer.declareStream(MEDIAN_INTERSECTION_STREAM, new Fields(INTERSECTION_MEDIAN_VEHICLES_OBJECT));
    }

}
