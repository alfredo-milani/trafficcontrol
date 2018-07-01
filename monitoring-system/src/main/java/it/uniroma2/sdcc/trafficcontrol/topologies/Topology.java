package it.uniroma2.sdcc.trafficcontrol.topologies;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import javax.validation.constraints.NotNull;


public abstract class Topology {

    private final TopologyBuilder builder;
    private final Config config;
    private final String topologyName;

    public Topology() {
        this.config = defineConfig();
        this.builder = defineTopology();
        this.topologyName = defineTopologyName();
    }

    public final Config createConfig() {
        return config;
    }

    protected @NotNull Config defineConfig() {
        return new Config();
    }

    public final StormTopology createTopology() {
        return builder.createTopology();
    }

    protected abstract @NotNull TopologyBuilder defineTopology() throws IllegalArgumentException;

    public final String createTopologyName() {
        return topologyName.replaceAll("\\p{Z}", "");
    }

    protected abstract @NotNull String defineTopologyName();

}
