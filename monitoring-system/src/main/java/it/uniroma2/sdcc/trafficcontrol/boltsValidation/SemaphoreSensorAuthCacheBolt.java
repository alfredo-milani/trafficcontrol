package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.RichSemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.CACHE_HIT_STREAM;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.CACHE_MISS_STREAM;

public class SemaphoreSensorAuthCacheBolt extends AbstractAuthenticationBolt {

    public SemaphoreSensorAuthCacheBolt(String cacheName) {
        super(cacheName);
    }

    @Override
    public void execute(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);
        String stream;

        // Verifica se il sensore è nella cache
        if (cacheManager.isKeyInCache(semaphoreSensor.getSemaphoreId())) {
            stream = CACHE_HIT_STREAM;
        } else {
            stream = CACHE_MISS_STREAM;
        }

        collector.emit(stream, new Values(semaphoreSensor));

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        Fields fields = new Fields(SEMAPHORE_SENSOR);

        declarer.declareStream(CACHE_HIT_STREAM, fields);
        declarer.declareStream(CACHE_MISS_STREAM, fields);
    }

}
