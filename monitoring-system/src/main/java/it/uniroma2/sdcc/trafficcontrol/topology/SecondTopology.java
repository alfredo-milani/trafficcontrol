package it.uniroma2.sdcc.trafficcontrol.topology;

import it.uniroma2.sdcc.trafficcontrol.bolt.AuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.bolt.AuthenticationCacheBolt;
import it.uniroma2.sdcc.trafficcontrol.bolt.ValidityCheckBolt;
import it.uniroma2.sdcc.trafficcontrol.firstQueryBolts.*;
import it.uniroma2.sdcc.trafficcontrol.secondQueryBolts.FiledsSelectorForMedian;
import it.uniroma2.sdcc.trafficcontrol.secondQueryBolts.FinalComparator;
import it.uniroma2.sdcc.trafficcontrol.secondQueryBolts.IntersectionAndGlobalMedian;
import it.uniroma2.sdcc.trafficcontrol.spout.KafkaSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.PARTIAL_RANK;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.REMOVE;
import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.ID;

public class SecondTopology {

    private final static String CLASS_NAME = SecondTopology.class.getName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    private final TopologyBuilder builder;

    public SecondTopology() {
        this.builder = new TopologyBuilder();
    }

/*
    public SecondTopology setRemoteTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout())
                .setNumTasks(4);
        builder.setBolt(VALIDITY_CHECK_BOLT, new ValidityCheckBolt())
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);
        builder.setBolt(AUTHENTICATION_BOLT, new AuthenticationBolt())
                .shuffleGrouping(VALIDITY_CHECK_BOLT)
                .setNumTasks(8);

        builder.setBolt(SELECTOR_BOLT_2, new FieldsSelectorForRanking())
                .fieldsGrouping(AUTHENTICATION_BOLT, new Fields(ID))
                .setNumTasks(4);
        builder.setBolt(FILTER_BOLT_QUERY_2, new FilterBolt2())
                .fieldsGrouping(SELECTOR_BOLT_2, new Fields(ID))
                .setNumTasks(4);
        builder.setBolt(PARTIAL_RANK, new PartialRankBolt(10))
                .fieldsGrouping(FILTER_BOLT_QUERY_2, new Fields(ID))
                .setNumTasks(4);
        builder.setBolt(GLOBAL_RANK, new GlobalRankBolt(10), 1)
                .allGrouping(PARTIAL_RANK, UPDATE)
                .allGrouping(PARTIAL_RANK, REMOVE)
                .setNumTasks(1);

        return this;
    }
*/

    public SecondTopology setLocalTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout())
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
                // .allGrouping(PARTIAL_RANK, UPDATE)
                // .allGrouping(PARTIAL_RANK, REMOVE)
                .globalGrouping(PARTIAL_RANK, UPDATE)
                .globalGrouping(PARTIAL_RANK, REMOVE)
                .setNumTasks(1);*/

        return this;
    }

    public StormTopology createTopology() {
        return builder.createTopology();
    }

    public static Logger getLOGGER() {
        return LOGGER;
    }


}
