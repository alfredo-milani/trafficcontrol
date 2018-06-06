package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.bolts.BaseDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.GlobalRankingsBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.MeanCalculatorBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.PartialRankingsBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;


public class FirstTopology extends BaseTopology {

    private final static String CLASS_NAME = FirstTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected void setConfig() {
        config.setNumWorkers(NUMBER_WORKERS_SELECTED);
        // Storm default: 1 for workers
        config.setNumAckers(NUMBER_WORKERS_SELECTED);
        // config.setMessageTimeoutSecs(80); // 10 sec in più rispetto alla lunghezza di finestra + interval
    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED), 2)
                .setNumTasks(4);

        builder.setBolt(BASE_DISPATCHER_BOLT, new BaseDispatcherBolt(), 2)
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);

        builder.setBolt(MEAN_CALCULATOR_BOLT, new MeanCalculatorBolt(), 2)
                .fieldsGrouping(BASE_DISPATCHER_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);
        /*
        builder.setBolt(PARTIAL_RANK_BOLT, new PartialRankWindowedBolt(10).withWindow(
                BaseWindowedBolt.Duration.minutes(1),
                BaseWindowedBolt.Duration.seconds(10)
                ), 2)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);
        builder.setBolt(PARTIAL_RANK_BOLT, new PartialRankBolt(10), 2)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);*/

        builder.setBolt(PARTIAL_RANK_BOLT, new PartialRankingsBolt(10, 1), 2)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);
        builder.setBolt(GLOBAL_RANK_BOLT, new GlobalRankingsBolt(10))
                .globalGrouping(PARTIAL_RANK_BOLT);
        /*builder.setBolt(GLOBAL_RANK_BOLT, new GlobalRankWindowedBolt(10).withWindow(
                BaseWindowedBolt.Duration.minutes(1),
                BaseWindowedBolt.Duration.seconds(10)
                ))
                .globalGrouping(PARTIAL_RANK_BOLT);*/
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
