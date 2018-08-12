package it.uniroma2.sdcc.trafficcontrol.topologies;


import it.uniroma2.sdcc.trafficcontrol.boltsValidation.*;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static it.uniroma2.sdcc.trafficcontrol.constants.CacheParams.MOBILE_AUTHENTICATION_CACHE_NAME;
import static it.uniroma2.sdcc.trafficcontrol.constants.CacheParams.SEMAPHORE_AUTHENTICATION_CACHE_NAME;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.MobileSensorTuple.MOBILE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.Params.Properties.NUMBER_OF_WORKERS;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class ValidationTopology extends Topology {

    private final static String CLASS_NAME = ValidationTopology.class.getSimpleName();

    @Override
    protected Config defineConfig() {
        Config config = new Config();

        config.setNumWorkers(NUMBER_OF_WORKERS);

        return config;
    }

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(GENERIC_TUPLE_TO_VALIDATE, CLASS_NAME), 6);

        builder.setBolt(VALIDATION_DISPATCHER_BOLT, new ValidationDispatcherBolt(), 6)
                .shuffleGrouping(KAFKA_SPOUT);


        builder.setBolt(SEMAPHORE_SENSOR_AUTH_CACHE_BOLT, new SemaphoreSensorAuthCacheBolt(SEMAPHORE_AUTHENTICATION_CACHE_NAME), 4)
                .fieldsGrouping(VALIDATION_DISPATCHER_BOLT, SEMAPHORE_SENSOR_STREAM, new Fields(SEMAPHORE_ID));
        builder.setBolt(MOBILE_SENSOR_AUTH_CACHE_BOLT, new MobileSensorAuthCacheBolt(MOBILE_AUTHENTICATION_CACHE_NAME), 4)
                .fieldsGrouping(VALIDATION_DISPATCHER_BOLT, MOBILE_SENSOR_STREAM, new Fields(MOBILE_ID));

        builder.setBolt(SEMAPHORE_AUTH_DB_BOLT, new SemaphoreAuthDBBolt(SEMAPHORE_AUTHENTICATION_CACHE_NAME), 6)
                .shuffleGrouping(SEMAPHORE_SENSOR_AUTH_CACHE_BOLT, CACHE_MISS_STREAM);
        builder.setBolt(MOBILE_AUTH_DB_BOLT, new MobileAuthDBBolt(MOBILE_AUTHENTICATION_CACHE_NAME), 6)
                .shuffleGrouping(MOBILE_SENSOR_AUTH_CACHE_BOLT, CACHE_MISS_STREAM);


        builder.setBolt(SEMAPHORE_VALIDATION_PUBLISHER_BOLT, new ValidationPublisherBolt(SEMAPHORE_SENSOR_VALIDATED), 4)
                .shuffleGrouping(SEMAPHORE_SENSOR_AUTH_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(SEMAPHORE_AUTH_DB_BOLT);
        builder.setBolt(MOBILE_VALIDATION_PUBLISHER_BOLT, new ValidationPublisherBolt(MOBILE_SENSOR_VALIDATED), 4)
                .shuffleGrouping(MOBILE_SENSOR_AUTH_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(MOBILE_AUTH_DB_BOLT);

        return builder;
    }

    @Override
    protected String defineTopologyName() {
        return CLASS_NAME;
    }

}
