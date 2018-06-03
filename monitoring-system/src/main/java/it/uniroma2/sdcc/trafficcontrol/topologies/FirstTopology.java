package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.FieldsSelectorForRanking;
import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.GlobalRankWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery.PartialRankWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.NUMBER_WORKERS_SELECTED;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;


public class FirstTopology extends Topology {

    private final static String CLASS_NAME = FirstTopology.class.getSimpleName();
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
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(VALIDATED), 2)
                .setNumTasks(4);

        builder.setBolt(FIELDS_SELECTION_FOR_RANKING, new FieldsSelectorForRanking(), 2)
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);

        builder.setBolt(PARTIAL_RANK, new PartialRankWindowedBolt(10).withWindow(
                BaseWindowedBolt.Duration.minutes(1),
                BaseWindowedBolt.Duration.seconds(10)
        ), 2)
                .fieldsGrouping(FIELDS_SELECTION_FOR_RANKING, new Fields(INTERSECTION_ID))
                .setNumTasks(4);
        /*builder.setBolt(PARTIAL_RANK, new PartialRankBolt(10), 2)
                .fieldsGrouping(FIELDS_SELECTION_FOR_RANKING, new Fields(INTERSECTION_ID))
                .setNumTasks(4);*/

        builder.setBolt(GLOBAL_RANK, new GlobalRankWindowedBolt(10).withWindow(
                BaseWindowedBolt.Duration.minutes(1),
                BaseWindowedBolt.Duration.seconds(10)
        ))
                .globalGrouping(PARTIAL_RANK);
    }

    @Override
    public String getClassName() {
        return "DioCane";
    }

    @Override
    public Logger getLOGGER() {
        return LOGGER;
    }

}
