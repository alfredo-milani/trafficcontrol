package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsValidation.*;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.CacheParams.MOBILE_AUTHENTICATION_CACHE_NAME;
import static it.uniroma2.sdcc.trafficcontrol.constants.CacheParams.SEMAPHORE_AUTHENTICATION_CACHE_NAME;
import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class ValidationTopology extends BaseTopology {

    private final static String CLASS_NAME = ValidationTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected void setConfig() {
        config.setNumWorkers(NUMBER_WORKERS_SELECTED);
    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(GENERIC_TUPLE_TO_VALIDATE), 4)
                .setNumTasks(8);

        builder.setBolt(
                VALIDATION_DISPATCHER_BOLT,
                new ValidationDispatcherBolt(),
                4
        )
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(8);


        builder.setBolt(SEMAPHORE_SENSOR_AUTH_CACHE_BOLT, new SemaphoreSensorAuthCacheBolt(SEMAPHORE_AUTHENTICATION_CACHE_NAME), 2)
                .fieldsGrouping(VALIDATION_DISPATCHER_BOLT, SEMAPHORE_SENSOR_STREAM, new Fields(SEMAPHORE_ID))
                .setNumTasks(4);
        builder.setBolt(MOBILE_SENSOR_AUTH_CACHE_BOLT, new MobileSensorAuthCacheBolt(MOBILE_AUTHENTICATION_CACHE_NAME), 2)
                .fieldsGrouping(VALIDATION_DISPATCHER_BOLT, MOBILE_SENSOR_STREAM, new Fields(MOBILE_ID))
                .setNumTasks(4);

        builder.setBolt(SEMAPHORE_AUTH_DB_BOLT, new SemaphoreAuthDBBolt(SEMAPHORE_AUTHENTICATION_CACHE_NAME), 6)
                .shuffleGrouping(SEMAPHORE_SENSOR_AUTH_CACHE_BOLT, CACHE_MISS_STREAM)
                .setNumTasks(6);
        builder.setBolt(MOBILE_AUTH_DB_BOLT, new MobileAuthDBBolt(MOBILE_AUTHENTICATION_CACHE_NAME), 6)
                .shuffleGrouping(MOBILE_SENSOR_AUTH_CACHE_BOLT, CACHE_MISS_STREAM)
                .setNumTasks(6);


        builder.setBolt(SEMAPHORE_VALIDATION_PUBLISHER_BOLT, new SemaphoreValidationPublisherBolt(SEMAPHORE_SENSOR_VALIDATED), 2)
                .shuffleGrouping(SEMAPHORE_SENSOR_AUTH_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(SEMAPHORE_AUTH_DB_BOLT)
                .setNumTasks(4);
        builder.setBolt(MOBILE_VALIDATION_PUBLISHER_BOLT, new SemaphoreValidationPublisherBolt(MOBILE_SENSOR_VALIDATED), 2)
                .shuffleGrouping(MOBILE_SENSOR_AUTH_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(MOBILE_AUTH_DB_BOLT)
                .setNumTasks(4);
    }

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }

    @Override
    public Logger getLOGGER() {
        return LOGGER;
    }

}
