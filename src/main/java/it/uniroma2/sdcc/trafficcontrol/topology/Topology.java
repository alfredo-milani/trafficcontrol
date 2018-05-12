package it.uniroma2.sdcc.trafficcontrol.topology;

import org.apache.storm.generated.StormTopology;

public abstract class Topology {

    protected Topology() {

    }

    public abstract StormTopology createTopology();
}