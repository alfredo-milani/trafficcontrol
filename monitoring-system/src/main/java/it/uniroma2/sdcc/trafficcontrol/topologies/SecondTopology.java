package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.FiledsSelectorForMedian;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.FinalComparator;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.IntersectionAndGlobalMedian;
import it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus.SemaphoreStatusBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.AuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.AuthenticationCacheBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.ValidityCheckBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class SecondTopology extends Topology {

    private final static String CLASS_NAME = SecondTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected void setConfig() {
        config.setNumWorkers(NUMBER_WORKERS_SELECTED);
        // Storm default: 1 for workers
        config.setNumAckers(NUMBER_WORKERS_SELECTED);
        config.setMessageTimeoutSecs(80); // 10 sec in più rispetto alla lunghezza di finestra + interval
    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(VALIDATED))
                .setNumTasks(4);
        builder.setBolt(VALIDITY_CHECK_BOLT, new ValidityCheckBolt())
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_CACHE_BOLT, new AuthenticationCacheBolt(), 4)
                .shuffleGrouping(VALIDITY_CHECK_BOLT)
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_BOLT, new AuthenticationBolt(), 3)
                .shuffleGrouping(AUTHENTICATION_CACHE_BOLT, CACHE_MISS_STREAM)
                .setNumTasks(6);
        builder.setBolt(SEMAPHORE_STATUS_BOLT, new SemaphoreStatusBolt())
                .shuffleGrouping(AUTHENTICATION_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(AUTHENTICATION_BOLT)
                .setNumTasks(4);

        builder.setBolt(FIELDS_SELECTION_FOR_MEDIAN, new FiledsSelectorForMedian())
                .shuffleGrouping(SEMAPHORE_STATUS_BOLT)
                .setNumTasks(4);

        builder.setBolt(INTERSECTION_AND_GLOBAL_MEDIAN, new IntersectionAndGlobalMedian())
                .shuffleGrouping(FIELDS_SELECTION_FOR_MEDIAN)
                .setNumTasks(1);
        builder.setBolt(FINAL_COMPARATOR,new FinalComparator())
                .allGrouping(UPDATE_GLOBAL_MEDIAN,UPDATE_GLOBAL_MEDIAN)
                .allGrouping(REMOVE_GLOBAL_MEDIAN_ITEM,REMOVE_GLOBAL_MEDIAN_ITEM)
                .allGrouping(VALUE_GLOBAL_MEDIAN,VALUE_GLOBAL_MEDIAN)
                .setNumTasks(1);

      /*  builder.setBolt(PARTIAL_RANK, new PartialRankBolt(10))
                // .fieldsGrouping(FIELDS_SELECTION_FOR_RANKING, new Fields(SEMAPHORE_STATUS))
                .shuffleGrouping(FIELDS_SELECTION_FOR_RANKING)
                .setNumTasks(4);
        builder.setBolt(GLOBAL_RANK, new GlobalRankBolt(10), 1)
                // .allGrouping(PARTIAL_RANK, UPDATE_PARTIAL)
                // .allGrouping(PARTIAL_RANK, REMOVE)
                .globalGrouping(PARTIAL_RANK, UPDATE_PARTIAL)
                .globalGrouping(PARTIAL_RANK, REMOVE)
                .setNumTasks(1);*/
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
