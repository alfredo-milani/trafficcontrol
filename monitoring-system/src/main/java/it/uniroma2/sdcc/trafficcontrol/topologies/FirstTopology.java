package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.*;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;


public class FirstTopology extends BaseTopology {

    private final static String CLASS_NAME = FirstTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected Config createConfig() {
        Config config = new Config();

        config.setNumWorkers(NUMBER_WORKERS_SELECTED);
        // Storm default: 1 for workers
        // config.setNumAckers(NUMBER_WORKERS_SELECTED);

        return config;
    }

    @Override
    protected TopologyBuilder setTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME), 4);
        builder.setBolt(MEAN_SPEED_DISPATCHER_BOLT, new MeanSpeedDispatcherBolt(), 4)
                .shuffleGrouping(KAFKA_SPOUT);

        // Bolt che calcola la velocità media di ogni intersezione
        builder.setBolt(MEAN_CALCULATOR_BOLT, new MeanCalculatorBolt(60, 4), 4)
                .fieldsGrouping(MEAN_SPEED_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));

        // Bolts gestori della finestra temporale da 15 minuti
        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_15_MIN, new PartialWindowedRankingsBolt(15 * 60, 2), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_15_MIN, new GlobalWindowedRankingsBolt(15 * 60, 5))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_15_MIN);
        // Bolts gestori della finestra temporale da 1 ora
        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_1_H, new PartialWindowedRankingsBolt(60 * 60, 2), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_1_H, new GlobalWindowedRankingsBolt(60 * 60, 5))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT_1_H);
        // Bolts gestori della finestra temporale da 24 ore
        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT_24_H, new PartialWindowedRankingsBolt(24 * 60 * 60, 2), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT_24_H, new GlobalWindowedRankingsBolt(24 * 60 * 60, 5))
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
    public String getClassName() {
        return CLASS_NAME;
    }

    @Override
    public Logger getLOGGER() {
        return LOGGER;
    }

}
