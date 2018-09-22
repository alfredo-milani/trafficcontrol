package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

import javax.validation.constraints.NotNull;
import java.util.UUID;


public abstract class Topology {

    private final TopologyBuilder builder;
    private final Config stormConfig;
    private final AppConfig appConfig;
    private final String topologyName;

    public Topology(AppConfig appConfig) {
        this.appConfig = appConfig;

        this.stormConfig = defineConfig();
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
        return stormConfig;
    }

    public final StormTopology createTopology() {
        return builder.createTopology();
    }

    public final String createTopologyName() {
        return topologyName.replaceAll("\\p{Z}", "");
    }

    public final AppConfig getAppConfig() {
        return appConfig;
    }
    // End - Interfaccia pubblica

}
