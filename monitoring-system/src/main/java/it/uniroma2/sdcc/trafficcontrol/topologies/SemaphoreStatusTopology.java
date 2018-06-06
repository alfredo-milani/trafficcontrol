package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus.SemaphoreStatusBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus.SemaphoreStatusPublisher;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_STATUS;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class SemaphoreStatusTopology extends BaseTopology {

    private final static String CLASS_NAME = SemaphoreStatusTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected void setConfig() {

    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(VALIDATED), 2)
                .setNumTasks(4);

        builder.setBolt(SEMAPHORE_STATUS_BOLT, new SemaphoreStatusBolt(), 2)
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);

        builder.setBolt(SEMAPHORE_STATUS_PUBLISHER_BOLT, new SemaphoreStatusPublisher(SEMAPHORE_STATUS))
                .shuffleGrouping(SEMAPHORE_STATUS_BOLT)
                .setNumTasks(2);
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
