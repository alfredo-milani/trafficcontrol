package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.*;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;


public class FirstTopology extends Topology {

    private final static String CLASS_NAME = FirstTopology.class.getSimpleName();

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME), 4);

        builder.setBolt(MEAN_SPEED_DISPATCHER_BOLT, new MeanSpeedDispatcherBolt(), 4)
                .shuffleGrouping(KAFKA_SPOUT);


        // Bolt che calcola la velocità media di ogni intersezione
        builder.setBolt(MEAN_CALCULATOR_BOLT, new MeanCalculatorBoltWindowed(TimeUnit.MINUTES.toSeconds(1), TimeUnit.SECONDS.toSeconds(2)), 4)
                .fieldsGrouping(MEAN_SPEED_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));

        // Bolts gestori della finestra temporale da 15 minuti
        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, new PartialWindowedRankingsBolt(TimeUnit.MINUTES.toSeconds(15), TimeUnit.SECONDS.toSeconds(5)), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_15_MIN, new GlobalWindowedRankingsBolt(TimeUnit.MINUTES.toSeconds(15), TimeUnit.SECONDS.toSeconds(5)))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_15_MIN);
        // Bolts gestori della finestra temporale da 1 ora
        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_1_H, new PartialWindowedRankingsBolt(TimeUnit.HOURS.toSeconds(1), TimeUnit.SECONDS.toSeconds(5)), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_1_H, new GlobalWindowedRankingsBolt(TimeUnit.HOURS.toSeconds(1), TimeUnit.SECONDS.toSeconds(5)))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_1_H);
        // Bolts gestori della finestra temporale da 24 ore
        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_24_H, new PartialWindowedRankingsBolt(TimeUnit.HOURS.toSeconds(24), TimeUnit.SECONDS.toSeconds(5)), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_24_H, new GlobalWindowedRankingsBolt(TimeUnit.HOURS.toSeconds(24), TimeUnit.SECONDS.toSeconds(5)))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_24_H);


        // Publisher bolt per la finestra temporale da 15 minuti
        builder.setBolt(RANK_PUBLISHER_BOLT_15_MIN, new RankPublisherBolt(RANKING_15_MIN))
                .shuffleGrouping(GLOBAL_WINDOWED_RANK_BOLT_15_MIN);
        // Publisher bolt per la finestra temporale da 1 ora
        builder.setBolt(RANK_PUBLISHER_BOLT_1_H, new RankPublisherBolt(RANKING_1_H))
                .shuffleGrouping(GLOBAL_WINDOWED_RANK_BOLT_1_H);
        // Publisher bolt per la finestra temporale da 24 ore
        builder.setBolt(RANK_PUBLISHER_BOLT_24_H, new RankPublisherBolt(RANKING_24_H))
                .shuffleGrouping(GLOBAL_WINDOWED_RANK_BOLT_24_H);

        return builder;
    }

    @Override
    protected String defineTopologyName() {
        return CLASS_NAME;
    }

}
