package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.FilterBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting.GreenSetter;
import it.uniroma2.sdcc.trafficcontrol.boltsValidation.ValidityCheckBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.tuple.Fields;

import java.util.logging.Logger;

import static com.sun.xml.internal.ws.spi.db.BindingContextFactory.LOGGER;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;
import static org.apache.logging.log4j.core.impl.ThrowableFormatOptions.CLASS_NAME;

public class GreenSettingTopology extends BaseTopology {

    private final static String CLASS_NAME = GreenSettingTopology.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    @Override
    protected void setConfig() {
        //vuota
    }

    @Override
    protected void setTopology() {
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(VALIDATED), 2)
                .setNumTasks(4);

        builder.setBolt(FILTER_GREEN_BOLT, new FilterBolt())
                .shuffleGrouping(KAFKA_SPOUT)
                .setNumTasks(4);



        builder.setBolt(GREEN_SETTER, new GreenSetter())
                .fieldsGrouping(FILTER_GREEN_BOLT, new Fields(INTERSECTION_ID))
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
