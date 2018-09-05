package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.boltsValidation.SemaphoreAuthByEndpointBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import it.uniroma2.sdcc.trafficcontrol.topologies.Topology;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.io.IOException;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.MODE_CLUSTER;
import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.MODE_LOCAL;

public class TopologyStarter {

    // File di configurazione
    private final static Config config;
    static {
        config = Config.getInstance();
        try {
            // Caricamento proprietà
            config.loadIfHasNotAlreadyBeenLoaded();
        } catch (IOException e) {
            System.err.println(String.format(
                    "%s: error while reading configuration file",
                    SemaphoreAuthByEndpointBolt.class.getSimpleName()
            ));
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(config.toString());

        // Creazione topologie
        List<Topology> topologies = config.getTopologiesToStart();

        switch (config.getMode()) {
            // Esecuzione storm in modalità locale
            case MODE_LOCAL:
                final LocalCluster cluster = new LocalCluster();

                topologies.forEach(t -> cluster.submitTopology(
                        t.createTopologyName(),
                        t.createConfig(),
                        t.createTopology()
                ));
                break;

            // Esecuzione storm in modalità cluster
            case MODE_CLUSTER:
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
                System.exit(config.getExitFailure());
        }
    }

}
