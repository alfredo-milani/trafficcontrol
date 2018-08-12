package it.uniroma2.sdcc.trafficcontrol.topologies;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import javax.validation.constraints.NotNull;
import java.util.UUID;


public abstract class Topology {

    private final TopologyBuilder builder;
    private final Config config;
    private final String topologyName;

    public Topology() {
        this.config = defineConfig();
        this.builder = defineTopology();
        this.topologyName = defineTopologyName();
    }

    // Template's methods
    protected @NotNull Config defineConfig() {
        return new Config();
    }

    protected abstract @NotNull TopologyBuilder defineTopology() throws IllegalArgumentException;

    protected @NotNull String defineTopologyName() {
        return UUID.randomUUID().toString();
    }
    // End - Template's methods

    // Interfaccia pubblica
    public final Config createConfig() {
        return config;
    }

    public final StormTopology createTopology() {
        return builder.createTopology();
    }

    public final String createTopologyName() {
        return topologyName.replaceAll("\\p{Z}", "");
    }
    // End - Interfaccia pubblica

}
