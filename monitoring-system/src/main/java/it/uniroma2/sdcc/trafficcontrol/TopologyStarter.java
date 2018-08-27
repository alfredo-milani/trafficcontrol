package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.topologies.*;
import it.uniroma2.sdcc.trafficcontrol.utils.ApplicationsProperties;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;


public class TopologyStarter {

    public static void main(String[] args)
            throws IOException {
        // Caricamento proprità dell'applicazione
        ApplicationsProperties.getInstance().loadProperties();

        // Creazione topologie
        List<Topology> topologies = Lists.newArrayList(
                new ValidationTopology(),
                new SemaphoreStatusTopology(),
                new FirstTopology(),
                new SecondTopology(),
                new ThirdTopology(),
                new GreenSettingTopology()
        );

        switch (ApplicationsProperties.MODE) {
            // Esecuzione storm in modalità locale
            case ApplicationsProperties.MODE_LOCAL:
                final LocalCluster cluster = new LocalCluster();

                topologies.forEach(t -> cluster.submitTopology(
                        t.createTopologyName(),
                        t.createConfig(),
                        t.createTopology()
                ));
                break;

            // Esecuzione storm in modalità cluster
            case ApplicationsProperties.MODE_CLUSTER:
                topologies.forEach(t -> {
                    try {
                        StormSubmitter.submitTopology(
                                t.createTopologyName(),
                                t.createConfig(),
                                t.createTopology()
                        );
                    } catch (AlreadyAliveException | AuthorizationException | InvalidTopologyException e) {
                        e.printStackTrace();
                    }
                });
                break;

            default:
                System.err.println("Errore sconosciuto");
                System.exit(ApplicationsProperties.EXIT_FAILURE);
        }
    }

}
