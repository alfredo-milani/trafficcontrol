package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.DirectionDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.MOBILE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.DIRECTION_DISPATCHER_BOLT;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.KAFKA_SPOUT;

public class ThirdTopology extends Topology {

    private static final String CLASS_NAME = ThirdTopology.class.getSimpleName();

    /*@Override
    protected Config defineConfig() {
        Config config = new Config();

        config.setNumWorkers(NUMBER_OF_WORKERS);

        return config;
    }*/

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(MOBILE_SENSOR_VALIDATED, CLASS_NAME), 4);
        builder.setBolt(DIRECTION_DISPATCHER_BOLT, new DirectionDispatcherBolt(), 4)
                .shuffleGrouping(KAFKA_SPOUT);



        return builder;
    }

    @Override
    protected String defineTopologyName() {
        return CLASS_NAME;
    }

}
