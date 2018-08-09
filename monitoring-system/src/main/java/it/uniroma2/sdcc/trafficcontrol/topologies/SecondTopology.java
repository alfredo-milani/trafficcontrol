package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.MedianCalculatorBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.MedianDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class SecondTopology extends Topology {

    private final static String CLASS_NAME = SecondTopology.class.getSimpleName();

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME))
                .setNumTasks(4);
        builder.setBolt(MEDIAN_VEHICLES_DISPATCHER_BOLT, new MedianDispatcherBolt(), 4)
                .shuffleGrouping(KAFKA_SPOUT);

        // Bolt che calcola la velocità media di ogni intersezione
        builder.setBolt(MEDIAN_CALCULATOR_BOLT, new MedianCalculatorBolt(60, 4), 4)
                .fieldsGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));


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

        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, new PartialRankBolt(10))
                // .fieldsGrouping(FIELDS_SELECTION_FOR_RANKING_BOLT, new Fields(SEMAPHORE_LIGHT_STATUS))
                .shuffleGrouping(FIELDS_SELECTION_FOR_RANKING_BOLT)
                .setNumTasks(4);
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_15_MIN, new GlobalRankBolt(10), 1)
                // .allGrouping(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, UPDATE_PARTIAL)
                // .allGrouping(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, REMOVE)
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, UPDATE_PARTIAL)
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, REMOVE)
                .setNumTasks(1);*/

        return builder;
    }

    @Override
    public String defineTopologyName() {
        return CLASS_NAME;
    }

}
