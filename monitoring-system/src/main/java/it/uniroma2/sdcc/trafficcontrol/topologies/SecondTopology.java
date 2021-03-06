package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.CongestedIntersectionsPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.GlobalMedianCalculatorBoltWindowed;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.MedianCalculatorBoltWindowed;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.MedianDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class SecondTopology extends Topology {

    private final static String CLASS_NAME = SecondTopology.class.getSimpleName();

    public SecondTopology(AppConfig appConfig) {
        super(appConfig);
    }

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(getAppConfig(), SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME),4);

        builder.setBolt(MEDIAN_VEHICLES_DISPATCHER_BOLT, new MedianDispatcherBolt(),4)
                .shuffleGrouping(KAFKA_SPOUT);


        // Bolt che calcola la mediana di ogni intersezione
        builder.setBolt(MEDIAN_CALCULATOR_BOLT, new MedianCalculatorBoltWindowed(TimeUnit.MINUTES.toSeconds(1), TimeUnit.SECONDS.toSeconds(2)),4)
                .fieldsGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT, SEMAPHORE_SENSOR_STREAM, new Fields(INTERSECTION_ID));

        // Bolt che calcola la mediana globale e riceve le mediane delle intersezioni per la finestra temporale di 15 minuti
        builder.setBolt(GLOBAL_MEDIAN_CALCULATOR_BOLT_15_MIN, new GlobalMedianCalculatorBoltWindowed(TimeUnit.MINUTES.toSeconds(15), TimeUnit.SECONDS.toSeconds(5)))
                .globalGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT, SEMAPHORE_SENSOR_STREAM)
                .globalGrouping(MEDIAN_CALCULATOR_BOLT, MEDIAN_INTERSECTION_STREAM);
        // Bolt che calcola la mediana globale e riceve le mediane delle intersezioni per la finestra temporale di 1 ora
        builder.setBolt(GLOBAL_MEDIAN_CALCULATOR_BOLT_1_H, new GlobalMedianCalculatorBoltWindowed(TimeUnit.HOURS.toSeconds(1), TimeUnit.SECONDS.toSeconds(5)))
                .globalGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT, SEMAPHORE_SENSOR_STREAM)
                .globalGrouping(MEDIAN_CALCULATOR_BOLT, MEDIAN_INTERSECTION_STREAM);
        // Bolt che calcola la mediana globale e riceve le mediane delle intersezioni per la finestra temporale di 24 ore
        builder.setBolt(GLOBAL_MEDIAN_CALCULATOR_BOLT_24_H, new GlobalMedianCalculatorBoltWindowed(TimeUnit.HOURS.toSeconds(24), TimeUnit.SECONDS.toSeconds(5)))
                .globalGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT, SEMAPHORE_SENSOR_STREAM)
                .globalGrouping(MEDIAN_CALCULATOR_BOLT, MEDIAN_INTERSECTION_STREAM);


        builder.setBolt(CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_15_MIN, new CongestedIntersectionsPublisherBolt(getAppConfig(), CONGESTED_INTERSECTIONS_15_MIN))
                .shuffleGrouping(GLOBAL_MEDIAN_CALCULATOR_BOLT_15_MIN);
        builder.setBolt(CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_1_H, new CongestedIntersectionsPublisherBolt(getAppConfig(), CONGESTED_INTERSECTIONS_1_H))
                .shuffleGrouping(GLOBAL_MEDIAN_CALCULATOR_BOLT_1_H);
        builder.setBolt(CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_24_H, new CongestedIntersectionsPublisherBolt(getAppConfig(), CONGESTED_INTERSECTIONS_24_H))
                .shuffleGrouping(GLOBAL_MEDIAN_CALCULATOR_BOLT_24_H);

        return builder;
    }

    @Override
    protected String defineTopologyName() {
        return CLASS_NAME;
    }

}
