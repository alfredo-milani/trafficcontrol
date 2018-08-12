package it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus;

import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.StatusSemaphoreSensor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_STATUS;

public class SemaphoreStatusBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = RichSemaphoreSensor.getInstanceFrom(tuple);
        if (semaphoreSensor != null) {
            StatusSemaphoreSensor semaphoreSensorStatus = StatusSemaphoreSensor.getInstanceFrom(semaphoreSensor);
            if (semaphoreSensorStatus.hasFaultyLamps()) {
                collector.emit(new Values(semaphoreSensorStatus));
            }
        } else {
            System.err.println(String.format("Bad tuple: %s", tuple.toString()));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_STATUS));
    }

}
