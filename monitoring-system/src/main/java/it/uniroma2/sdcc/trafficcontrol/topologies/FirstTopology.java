package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.*;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
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
    protected void setConfig() {
        config.setNumWorkers(NUMBER_WORKERS_SELECTED);
        // Storm default: 1 for workers
        config.setNumAckers(NUMBER_WORKERS_SELECTED);
        // config.setMessageTimeoutSecs(80); // 10 sec in più rispetto alla lunghezza di finestra + interval
    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME), 2)
                .setNumTasks(4);
        builder.setBolt(MEAN_SPEED_DISPATCHER_BOLT, new MeanSpeedDispatcherBolt(), 2)
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);

        builder.setBolt(MEAN_CALCULATOR_BOLT, new MeanCalculatorBolt(9, 9), 4)
                .fieldsGrouping(MEAN_SPEED_DISPATCHER_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);

        builder.setBolt(PARTIAL_WINDOWED_RANK_BOLT, new PartialWindowedRankingsBolt(6, 1), 4)
                .fieldsGrouping(MEAN_CALCULATOR_BOLT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);
        builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT, new GlobalWindowedRankingsBolt(6, 2))
                .globalGrouping(PARTIAL_WINDOWED_RANK_BOLT);

        builder.setBolt(MOBILE_VALIDATION_PUBLISHER_BOLT, new GlobalRankingsPublisherBolt(RANKINGS_PROCESSED), 2)
                .shuffleGrouping(GLOBAL_WINDOWED_RANK_BOLT)
                .setNumTasks(2);


        /*builder.setBolt(GLOBAL_WINDOWED_RANK_BOLT, new GlobalWindowedRankingsBolt(6, 2))
                .globalGrouping(MEAN_SPEED_DISPATCHER_BOLT);*/
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
