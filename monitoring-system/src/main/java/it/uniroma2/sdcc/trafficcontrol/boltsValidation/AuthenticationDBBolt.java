package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.utils.EhCacheManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.CacheParams.SEMAPHORE_AUTHENTICATION_CACHE_NAME;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class AuthenticationDBBolt extends BaseRichBolt {

    private OutputCollector collector;
    private EhCacheManager cacheManager;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.cacheManager = new EhCacheManager(SEMAPHORE_AUTHENTICATION_CACHE_NAME);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
            Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
            Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
            Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
            Long semaphoreTimestampUTC = tuple.getLongByField(SEMAPHORE_TIMESTAMP_UTC);
            Short greenLightDuration = tuple.getShortByField(GREEN_LIGHT_DURATION);
            Byte greenLightStatus = tuple.getByteByField(GREEN_LIGHT_STATUS);
            Byte yellowLightStatus = tuple.getByteByField(YELLOW_LIGHT_STATUS);
            Byte redLightStatus = tuple.getByteByField(RED_LIGHT_STATUS);
            Short vehiclesPerSecond = tuple.getShortByField(VEHICLES);
            Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);

            Values values = new Values(
                    intersectionId,
                    semaphoreId,
                    semaphoreLatitude,
                    semaphoreLongitude,
                    semaphoreTimestampUTC,
                    greenLightDuration,
                    greenLightStatus,
                    yellowLightStatus,
                    redLightStatus,
                    vehiclesPerSecond,
                    averageVehiclesSpeed
            );

            boolean semaphoreInSystem;
            synchronized (cacheManager.getCacheManager()) {
                // Double checked lock
                if (!(semaphoreInSystem = cacheManager.isKeyInCache(semaphoreId))) {
                    if (semaphoreInSystem = RESTfulAPI.semaphoreSensorExist(semaphoreId)) {
                        cacheManager.put(semaphoreId, tuple.getSourceStreamId());
                    }
                }
            }

            if (semaphoreInSystem) {
                collector.emit(values);
            }
        } catch (ClassCastException | IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                SEMAPHORE_ID,
                SEMAPHORE_LATITUDE,
                SEMAPHORE_LONGITUDE,
                SEMAPHORE_TIMESTAMP_UTC,
                GREEN_LIGHT_DURATION,
                GREEN_LIGHT_STATUS,
                YELLOW_LIGHT_STATUS,
                RED_LIGHT_STATUS,
                VEHICLES,
                AVERAGE_VEHICLES_SPEED
        ));
    }

}
