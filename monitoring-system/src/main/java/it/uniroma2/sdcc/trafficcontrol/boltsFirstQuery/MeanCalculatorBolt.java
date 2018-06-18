package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MeanIntersectoinSpeedNotReady;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;


public class MeanCalculatorBolt extends AbstractWindowedBolt {

    private final static String CLASS_NAME = MeanCalculatorBolt.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

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
        eventsWindow.getExpiredEvents().forEach(t -> meanSpeedIntersectionQueue.remove(
                ((RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR)).getIntersectionId()
        ));

        eventsWindow.getNewEvents().forEach(t -> {
            RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR);
            Long intersectionId = richSemaphoreSensor.getIntersectionId();
            SemaphoreSensor semaphoreSensor = SemaphoreSensor.getInstanceFrom(richSemaphoreSensor);

            // Se la chiave è presente ritorna l'istanza dalla hashMap,
            // altrimenti aggiungi il valore nella hashMap e ritorna null
            MeanSpeedIntersection intersectionFromHashMap = meanSpeedIntersectionQueue.putIfAbsent(
                    intersectionId,
                    new MeanSpeedIntersection(intersectionId, semaphoreSensor)
            );

            if (intersectionFromHashMap != null) { // Intersezione da aggiornare
                intersectionFromHashMap.addSemaphoreSensor(semaphoreSensor);
                try {
                    intersectionFromHashMap.computeMeanIntersectionSpeed();
                    collector.emit(new Values(intersectionId, meanSpeedIntersectionQueue.remove(intersectionId)));
                } catch (MeanIntersectoinSpeedNotReady e) {
                    // Non sono ancora arrivate tutte le tuple per computare la media
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
                INTERSECTION_MEAN_SPEED_OBJECT
        ));
    }

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }

    @Override
    public Logger getLogger() {
        return LOGGER;
    }

}
