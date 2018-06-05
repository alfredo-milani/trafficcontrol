package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsValidation.AuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.AuthenticationCacheBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.ValidationPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.ValidityCheckBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.TO_VALIDATE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
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
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(TO_VALIDATE), 2)
                .setNumTasks(4);

        builder.setBolt(VALIDITY_CHECK_BOLT, new ValidityCheckBolt(), 2)
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_CACHE_BOLT, new AuthenticationCacheBolt(), 4)
                .fieldsGrouping(VALIDITY_CHECK_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_BOLT, new AuthenticationBolt(), 6)
                .shuffleGrouping(AUTHENTICATION_CACHE_BOLT, CACHE_MISS_STREAM)
                .setNumTasks(6);
        builder.setBolt(VALIDATION_PUBLISHER_BOLT, new ValidationPublisherBolt(), 2)
                .shuffleGrouping(AUTHENTICATION_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(AUTHENTICATION_BOLT)
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
