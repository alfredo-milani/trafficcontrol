package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersectionManager;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.timeWindow.ITimeWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GlobalMedianCalculatorBoltWindowed extends AbstractWindowedBolt {

    private final Map<Long, RichSemaphoreSensor> globalIntersections;
    private final MedianIntersectionManager medianIntersectionManager;

    public GlobalMedianCalculatorBoltWindowed(int windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public GlobalMedianCalculatorBoltWindowed(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        globalIntersections = new HashMap<>();
        medianIntersectionManager = new MedianIntersectionManager();
    }

    @Override
    protected void onTick(OutputCollector collector, ITimeWindow<Tuple> eventsWindow) {
        Map<Long, MedianIntersection> oldHigherMedianIntersection = new HashMap<>(medianIntersectionManager.getHigherMedianIntersection());

        // Elimino dati scaduti provenienti dai due streams
        eventsWindow.getExpiredEvents().forEach(t -> {
            if (t.getSourceStreamId().equals(SEMAPHORE_SENSOR_STREAM)) {
                RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR);
                globalIntersections.remove(richSemaphoreSensor.getSemaphoreId(), richSemaphoreSensor);
            } else if (t.getSourceStreamId().equals(MEDIAN_INTERSECTION_STREAM)) {
                MedianIntersection medianIntersection = (MedianIntersection) t.getValueByField(INTERSECTION_MEDIAN_VEHICLES_OBJECT);
                medianIntersectionManager.remove(medianIntersection.getIntersectionId(), medianIntersection);
            }
        });

        // Aggiorno la mediana globale
        eventsWindow.getNewEvents().forEach(t -> {
            if (t.getSourceStreamId().equals(SEMAPHORE_SENSOR_STREAM)) {
                RichSemaphoreSensor richSemaphoreSensor = (RichSemaphoreSensor) t.getValueByField(SEMAPHORE_SENSOR);
                // Tengo solo il valore più aggiornato dei semafori con stesso id per ottenere
                // informazioni sulla congestione in tempo reale delle intersezioni
                globalIntersections.put(richSemaphoreSensor.getSemaphoreId(), richSemaphoreSensor);
            }
        });
        medianIntersectionManager.updateMedianFrom(globalIntersections);

        // Aggiorno le intersezioni la cui mediana è superiore a quella globale
        eventsWindow.getNewEvents().forEach(t -> {
            if (t.getSourceStreamId().equals(MEDIAN_INTERSECTION_STREAM)) {
                MedianIntersection medianIntersection = (MedianIntersection) t.getValueByField(INTERSECTION_MEDIAN_VEHICLES_OBJECT);
                medianIntersectionManager.addIntersectionIfNeeded(medianIntersection);
            }
        });

        // Invio nuovi dati solo se ci sono stati aggiornamenti nella lista delle
        // intersezioni la cui mediana è maggiore rispetto a quella globale
        if (!oldHigherMedianIntersection.equals(medianIntersectionManager.getHigherMedianIntersection())) {
            medianIntersectionManager.sort();
            collector.emit(new Values(medianIntersectionManager));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(CONGESTED_INTERSECTIONS));
    }

}
