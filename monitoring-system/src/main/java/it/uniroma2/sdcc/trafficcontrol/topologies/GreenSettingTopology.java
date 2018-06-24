package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.FilterBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.GreenSetter;
import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.GreenTimingDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.GREEN_TEMPORIZATION;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GreenSettingTopology extends Topology {

    private final static String CLASS_NAME = GreenSettingTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected TopologyBuilder setTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED, CLASS_NAME), 4);

        builder.setBolt(GREEN_TIMING_DISPATCHER_BOLT, new GreenTimingDispatcherBolt(), 2)
                .shuffleGrouping(KAFKA_SPOUT);

        builder.setBolt(FILTER_GREEN_BOLT, new FilterBolt(), 2)
                .fieldsGrouping(GREEN_TIMING_DISPATCHER_BOLT, new Fields(INTERSECTION_ID));

        builder.setBolt(GREEN_SETTER, new GreenSetter(GREEN_TEMPORIZATION), 2)
                .shuffleGrouping(FILTER_GREEN_BOLT);

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
