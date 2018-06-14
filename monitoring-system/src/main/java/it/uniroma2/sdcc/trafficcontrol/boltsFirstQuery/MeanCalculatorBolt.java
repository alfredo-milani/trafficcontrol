package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
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


public class MeanCalculatorBolt extends AbstractWindowedBolt {

    private final Map<Long, MeanSpeedIntersection> meanSpeedIntersectionQueue;

    public MeanCalculatorBolt(int windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public MeanCalculatorBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        this.meanSpeedIntersectionQueue = new HashMap<>();
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        eventsWindow.getExpiredEventsWindow().forEach(t -> {
            RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR);
            Long intersectionId = richSemaphoreSensor.getIntersectionId();

            meanSpeedIntersectionQueue.remove(intersectionId);
        });

        eventsWindow.getNewEventsWindow().forEach(t -> {
            RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR);
            Long intersectionId = richSemaphoreSensor.getIntersectionId();
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getInstanceFrom(richSemaphoreSensor);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            MeanSpeedIntersection intersectionFromHashMap = meanSpeedIntersectionQueue.putIfAbsent(
                    intersectionId,
                    new MeanSpeedIntersection(intersectionId)
            );

            if (intersectionFromHashMap != null) { // Intersezione da aggiornare
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);
                if (intersectionFromHashMap.isListReadyForComputation()) { // Controllo se sono arrivate tutte le tuple per computare la media
                    intersectionFromHashMap.computeIntersectionMeanSpeed();
                    if (intersectionFromHashMap.isMeanComputed()) {
                        collector.emit(new Values(intersectionId, meanSpeedIntersectionQueue.remove(intersectionId)));
                    }
                }
            }
        });
    }

    @Override
    protected Long getTimestampFrom(Tuple tuple) {
        RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);
        return richSemaphoreSensor.getSemaphoreTimestampUTC();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                INTERSECTION_MEAN_SPEED_OBJECT
        ));
    }

}
