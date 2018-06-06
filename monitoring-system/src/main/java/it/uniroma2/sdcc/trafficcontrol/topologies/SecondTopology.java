package it.uniroma2.sdcc.trafficcontrol.topologies;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;

public class SecondTopology extends BaseTopology {

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
        /*builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED))
                .setNumTasks(4);
        builder.setBolt(BASE_DISPATCHER_BOLT, new BaseDispatcherBolt())
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_CACHE_BOLT, new SemaphoreSensorAuthCacheBolt(), 4)
                .shuffleGrouping(BASE_DISPATCHER_BOLT)
                .setNumTasks(4);
        builder.setBolt(SEMAPHORE_AUTH_DB_BOLT, new AuthenticationDBBolt(), 3)
                .shuffleGrouping(AUTHENTICATION_CACHE_BOLT, CACHE_MISS_STREAM)
                .setNumTasks(6);
        builder.setBolt(SEMAPHORE_STATUS_BOLT, new SemaphoreStatusBolt())
                .shuffleGrouping(AUTHENTICATION_CACHE_BOLT, CACHE_HIT_STREAM)
                .shuffleGrouping(SEMAPHORE_AUTH_DB_BOLT)
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
                .setNumTasks(1);*/

      /*  builder.setBolt(PARTIAL_RANK_BOLT, new PartialRankBolt(10))
                // .fieldsGrouping(FIELDS_SELECTION_FOR_RANKING_BOLT, new Fields(SEMAPHORE_STATUS))
                .shuffleGrouping(FIELDS_SELECTION_FOR_RANKING_BOLT)
                .setNumTasks(4);
        builder.setBolt(GLOBAL_RANK_BOLT, new GlobalRankBolt(10), 1)
                // .allGrouping(PARTIAL_RANK_BOLT, UPDATE_PARTIAL)
                // .allGrouping(PARTIAL_RANK_BOLT, REMOVE)
                .globalGrouping(PARTIAL_RANK_BOLT, UPDATE_PARTIAL)
                .globalGrouping(PARTIAL_RANK_BOLT, REMOVE)
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
