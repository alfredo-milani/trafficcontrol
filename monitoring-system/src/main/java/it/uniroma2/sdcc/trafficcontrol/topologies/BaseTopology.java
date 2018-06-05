package it.uniroma2.sdcc.trafficcontrol.topologies;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import java.util.logging.Logger;

public abstract class BaseTopology {

    final TopologyBuilder builder;
    final Config config;

    BaseTopology() {
        this.builder = new TopologyBuilder();
        this.config = new Config();

        // Template pattern
        setConfig();
        setTopology();
    }

    protected abstract void setConfig();

    protected abstract void setTopology();

    public final StormTopology createTopology() {
        return builder.createTopology();
    }

    public final Config getConfig() {
        return config;
    }

    public abstract String getClassName();

    public abstract Logger getLOGGER();

}
