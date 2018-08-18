package it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequence;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.SEMAPHORE_SEQUENCE_OBJECT;

public class CongestionComputationWindowedBolt extends AbstractWindowedBolt {

    private final SemaphoresSequence semaphoresSequence;

    public CongestionComputationWindowedBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        semaphoresSequence = new SemaphoresSequence();
    }

    public CongestionComputationWindowedBolt(int windowSizeInSeconds, int emitFrequencyInSeconds,
                                             SemaphoresSequence semaphoresSequence) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);
        this.semaphoresSequence = semaphoresSequence;
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        Double oldCongestionGrade = semaphoresSequence.getCongestionGrade();

        eventsWindow.getExpiredEvents().forEach(t -> {
            RichMobileSensor richMobileSensor = (RichMobileSensor) t.getValueByField(MOBILE_SENSOR);
            semaphoresSequence.getSensorsInSequence().remove(richMobileSensor);
        });

        eventsWindow.getNewEvents().forEach(t -> {
            RichMobileSensor richMobileSensor = (RichMobileSensor) t.getValueByField(MOBILE_SENSOR);
            semaphoresSequence.getSensorsInSequence().add(richMobileSensor);
        });

        semaphoresSequence.computeCongestionGrade();
        if (!semaphoresSequence.getCongestionGrade().equals(oldCongestionGrade)) {
            collector.emit(new Values(semaphoresSequence.createCopyToSend()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_SEQUENCE_OBJECT));
    }

}