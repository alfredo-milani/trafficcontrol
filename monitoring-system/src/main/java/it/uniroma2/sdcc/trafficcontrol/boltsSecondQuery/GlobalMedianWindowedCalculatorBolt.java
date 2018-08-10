package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.MedianIntersectionNotReady;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GlobalMedianWindowedCalculatorBolt extends AbstractWindowedBolt {

    private double compression = 100;
    private double quantile = 0.5; //mediana
    private final ArrayList<MedianIntersection> aboveMedianGlobalQueue;


    public GlobalMedianWindowedCalculatorBolt(int windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);

    }

    public GlobalMedianWindowedCalculatorBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        this.aboveMedianGlobalQueue = new ArrayList<>();
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {

        eventsWindow.getExpiredEvents().forEach(t -> aboveMedianGlobalQueue.remove(
                t.getValueByField(INTERSECTION_MEDIAN_VEHICLES_OBJECT)
        ));


        ArrayList<SemaphoreSensor> semaphoreSensors = new ArrayList<>();
        ArrayList<RichSemaphoreSensor> richSemaphoreSensors = new ArrayList<>();
        ArrayList<Tuple> currentEvents = eventsWindow.getCurrentEvents();

        TDigest tDigestIntesection = TDigest.createAvlTreeDigest(compression);
        for(int i=0; i<currentEvents.size(); i++){
            if (currentEvents.get(i).getSourceStreamId().equals(GLOBAL_MEDIAN_CALCULATOR_STREAM)) {
                richSemaphoreSensors.add((RichSemaphoreSensor) currentEvents.get(i).getValueByField(SEMAPHORE_SENSOR));
                semaphoreSensors.add(SemaphoreSensor.getInstanceFrom(richSemaphoreSensors.get(i)));
                tDigestIntesection.add(semaphoreSensors.get(i).getVehiclesNumber());
            }
        }
        double globalMedian= tDigestIntesection.quantile(quantile);

        eventsWindow.getNewEvents().forEach(t -> {
            if(t.getSourceStreamId().equals(MEDIAN_INTERSECTION_VALUE_STREAM)) {
                MedianIntersection medianIntersection = MedianIntersection.getIstanceFrom(t);

                if(medianIntersection.getMedianIntersectionCalculated()>globalMedian) {
                    aboveMedianGlobalQueue.add(medianIntersection);
                    try {
                        collector.emit(new Values(aboveMedianGlobalQueue));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

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
                CONGESTED_INTERSECTIONS
        ));
    }
}
