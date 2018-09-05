package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SemaphoresSequence;
import it.uniroma2.sdcc.trafficcontrol.entity.timeWindow.IClientTimeWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SEQUENCE_OBJECT;

public class CongestionComputationWindowedBolt extends AbstractWindowedBolt {

    private final SemaphoresSequence semaphoresSequence;

    public CongestionComputationWindowedBolt(long windowSizeInSeconds, long emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        semaphoresSequence = new SemaphoresSequence();
    }

    public CongestionComputationWindowedBolt(long windowSizeInSeconds, long emitFrequencyInSeconds,
                                             SemaphoresSequence semaphoresSequence) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        this.semaphoresSequence = semaphoresSequence;
    }

    @Override
    protected void onTick(OutputCollector collector, IClientTimeWindow<Tuple> eventsWindow) {
        Double oldCongestionGrade = semaphoresSequence.getCongestionGrade();

        eventsWindow.getExpiredEvents().forEach(t -> {
            RichMobileSensor richMobileSensor = (RichMobileSensor) t.getValueByField(MOBILE_SENSOR);
            semaphoresSequence.removeSensorInSequence(richMobileSensor);
        });

        eventsWindow.getNewEvents().forEach(t -> {
            RichMobileSensor richMobileSensor = (RichMobileSensor) t.getValueByField(MOBILE_SENSOR);
            semaphoresSequence.addSensorInSequence(richMobileSensor);
        });

        semaphoresSequence.computeCongestionGrade();
        if (!semaphoresSequence.getCongestionGrade().equals(oldCongestionGrade)) {
            collector.emit(new Values(semaphoresSequence.lightweightCopy()));
        }
    }

    @Override
    protected Long getTimestampFrom(Tuple tuple) {
        RichMobileSensor richMobileSensor = (RichMobileSensor) tuple.getValueByField(MOBILE_SENSOR);
        return richMobileSensor.getMobileTimestampUTC();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_SEQUENCE_OBJECT));
    }

}
