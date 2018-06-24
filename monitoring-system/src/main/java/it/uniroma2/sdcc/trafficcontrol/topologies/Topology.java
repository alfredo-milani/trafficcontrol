package it.uniroma2.sdcc.trafficcontrol.topologies;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.logging.Logger;

public abstract class Topology {

    private final TopologyBuilder builder;
    private final Config config;

    public Topology() throws IllegalArgumentException {
        this.config = createConfig();
        this.builder = setTopology();
    }

    protected Config createConfig() {
        return new Config();
    }

    protected abstract TopologyBuilder setTopology();

    public StormTopology createTopology() {
        return builder.createTopology();
    }

    public Config getConfig() {
        return config;
    }

    public abstract String getClassName();

    public abstract Logger getLOGGER();

}
