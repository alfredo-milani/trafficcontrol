package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.CongestedIntersectionsPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.GlobalMedianWindowedCalculatorBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.MedianWindowedCalculatorBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery.MedianDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.CONGESTED_INTERSECTIONS_15_MIN;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class SecondTopology extends Topology {

    //per definire il suo group id
    private final static String CLASS_NAME = SecondTopology.class.getSimpleName();

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME))
                .setNumTasks(4);
        builder.setBolt(MEDIAN_VEHICLES_DISPATCHER_BOLT, new MedianDispatcherBolt(), 4)
                .shuffleGrouping(KAFKA_SPOUT);

        // Bolt che calcola la mediana di ogni intersezione
        builder.setBolt(MEDIAN_CALCULATOR_BOLT_15_MIN, new MedianWindowedCalculatorBolt(15*60, 4), 4)
                .fieldsGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));
        // Bolt che calcola la mediana globale e riceve le mediane delle intersezioni
        builder.setBolt(GLOBAL_MEDIAN_CALCULATOR_BOLT_15_MIN, new GlobalMedianWindowedCalculatorBolt(15*60, 4), 1)
                .shuffleGrouping(MEDIAN_VEHICLES_DISPATCHER_BOLT).shuffleGrouping(MEDIAN_CALCULATOR_BOLT_15_MIN);

        builder.setBolt(CONGESTED_INTERSECTIONS_PUBLISHER_BOLT_15_MIN, new CongestedIntersectionsPublisherBolt(CONGESTED_INTERSECTIONS_15_MIN), 4)
                .shuffleGrouping(GLOBAL_MEDIAN_CALCULATOR_BOLT_15_MIN);


        return builder;
    }

    @Override
    public String defineTopologyName() {
        return CLASS_NAME;
    }

}
