package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.*;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.RANKINGS_PROCESSED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
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

        builder.setBolt(MEAN_CALCULATOR_BOLT, new MeanCalculatorBolt(6, 6), 4)
                .fieldsGrouping(MEAN_SPEED_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));

        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT, new PartialWindowedRankingsBolt(6, 1), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID));
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT, new GlobalWindowedRankingsBolt(6, 2))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT);

        builder.setBolt(MOBILE_VALIDATION_PUBLISHER_BOLT, new GlobalRankingsPublisherBolt(RANKINGS_PROCESSED))
                .shuffleGrouping(GLOBAL_WINDOWED_RANK_BOLT);

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
