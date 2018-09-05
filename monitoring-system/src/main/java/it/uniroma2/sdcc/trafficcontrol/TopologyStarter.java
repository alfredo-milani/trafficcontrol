package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import it.uniroma2.sdcc.trafficcontrol.topologies.*;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.*;

public class TopologyStarter {

    @SuppressWarnings("unchecked")
    public static void main(String[] args)
            throws IOException {
        // Caricamento proprità dell'applicazione
        Config config = Config.getInstance();
        config.load();
        System.out.println(config.toString());

        // Creazione topologie
        List<Topology> topologies = new ArrayList<>();
        List<String> topologiesToStart = (List<String>) config.get(TOPOLOGIES_TO_START);
        if (topologiesToStart.size() == 1 && topologiesToStart.contains(TOPOLOGIES_ALL)) {
            topologies.add(new ValidationTopology());
            topologies.add(new SemaphoreStatusTopology());
            topologies.add(new FirstTopology());
            topologies.add(new SecondTopology());
            topologies.add(new ThirdTopology());
            topologies.add(new GreenTimingTopology());
        } else {
            topologiesToStart.forEach(s -> {
                if (s.equals(TOPOLOGY_VALIDATION))              topologies.add(new ValidationTopology());
                else if (s.equals(TOPOLOGY_SEMAPHORE_STATUS))   topologies.add(new SemaphoreStatusTopology());
                else if (s.equals(TOPOLOGY_FIRST))              topologies.add(new FirstTopology());
                else if (s.equals(TOPOLOGY_SECOND))             topologies.add(new SecondTopology());
                else if (s.equals(TOPOLOGY_THIRD))              topologies.add(new ThirdTopology());
                else if (s.equals(TOPOLOGY_GREEN_TIMING))       topologies.add(new GreenTimingTopology());
                else System.err.println(String.format("Topologia sconosciuta: \"%s\"", s));
            });
        }

        switch ((String) config.get(MODE)) {
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
                System.exit((int) config.get(EXIT_FAILURE));
        }
    }

}
