package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.FilterBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.GreenSetter;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.GREEN_TEMPORIZATION;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SEMAPHORE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GreenSettingTopology extends BaseTopology {

    private final static String CLASS_NAME = GreenSettingTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected void setConfig() {

    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(SEMAPHORE_SENSOR_VALIDATED), 2)
                .setNumTasks(4);

        builder.setBolt(FILTER_GREEN_BOLT, new FilterBolt())
                .fieldsGrouping(KAFKA_SPOUT, new Fields(INTERSECTION_ID))
                .setNumTasks(4);

        builder.setBolt(GREEN_SETTER, new GreenSetter(GREEN_TEMPORIZATION))
                .shuffleGrouping(FILTER_GREEN_BOLT)
                .setNumTasks(4);
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
