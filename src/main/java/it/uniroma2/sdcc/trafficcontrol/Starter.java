package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.topology.FirstTopology;
import it.uniroma2.sdcc.trafficcontrol.topology.Topology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

public class Starter {

    public static void main(String[] args) {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        Topology topology = new FirstTopology();
        cluster.submitTopology("first_topology", conf, topology.createTopology());
    }

}
