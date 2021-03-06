package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.DirectionWaiterBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.GreenSetterPublisher;
import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.GreenTimingDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.GREEN_TEMPORIZATION;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GreenTimingTopology extends Topology {

    private final static String CLASS_NAME = GreenTimingTopology.class.getSimpleName();

    public GreenTimingTopology(AppConfig appConfig) {
        super(appConfig);
    }

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(getAppConfig(), SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME), 4);

        builder.setBolt(GREEN_TIMING_DISPATCHER_BOLT, new GreenTimingDispatcherBolt(), 2)
                .shuffleGrouping(KAFKA_SPOUT);

        builder.setBolt(FILTER_GREEN_BOLT, new DirectionWaiterBolt(), 2)
                .fieldsGrouping(GREEN_TIMING_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));

        builder.setBolt(GREEN_SETTER, new GreenSetterPublisher(getAppConfig(), GREEN_TEMPORIZATION), 2)
                .shuffleGrouping(FILTER_GREEN_BOLT);

        return builder;
    }

    @Override
    protected String defineTopologyName() {
        return CLASS_NAME;
    }

}
