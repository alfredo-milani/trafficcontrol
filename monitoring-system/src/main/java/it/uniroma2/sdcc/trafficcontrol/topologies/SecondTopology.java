package it.uniroma2.sdcc.trafficcontrol.topologies;

import org.apache.storm.topology.TopologyBuilder;

import java.util.logging.Logger;

public class SecondTopology extends BaseTopology {

    private final static String CLASS_NAME = SecondTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected TopologyBuilder setTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        /*builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME))
                .setNumTasks(4);
        builder.setBolt(MEAN_SPEED_DISPATCHER_BOLT, new MeanSpeedDispatcherBolt())
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_CACHE_BOLT, new SemaphoreSensorAuthCacheBolt(), 4)
                .shuffleGrouping(MEAN_SPEED_DISPATCHER_BOLT)
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
                .setNumTasks(1);

        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT, new PartialRankBolt(10))
                // .fieldsGrouping(FIELDS_SELECTION_FOR_RANKING_BOLT, new Fields(SEMAPHORE_LIGHT_STATUS))
                .shuffleGrouping(FIELDS_SELECTION_FOR_RANKING_BOLT)
                .setNumTasks(4);
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT, new GlobalRankBolt(10), 1)
                // .allGrouping(PARTIAL_WINDOWED_RANK_BOLT, UPDATE_PARTIAL)
                // .allGrouping(PARTIAL_WINDOWED_RANK_BOLT, REMOVE)
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT, UPDATE_PARTIAL)
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT, REMOVE)
                .setNumTasks(1);*/

        return builder;
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
