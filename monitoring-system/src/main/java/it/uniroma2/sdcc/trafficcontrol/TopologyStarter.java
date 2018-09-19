package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.io.IOException;

import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.MODE_CLUSTER;
import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.MODE_LOCAL;

public class TopologyStarter {

    // File di configurazione
    private final static Config config = Config.getInstance();

    public static void main(String[] args)
            throws IOException {
        // Controllo se è stato passato un file di configurazione per
        // l'applicazione da linea di comando
        if (args.length != 0) config.load(args[0]);
        else config.load();

        // Stampa configurazione attuale
        System.out.println(config.toString());

        switch (config.getMode()) {
            // Esecuzione storm in modalità locale
            case MODE_LOCAL:
                final LocalCluster cluster = new LocalCluster();

                config.getTopologies().forEach(t -> cluster.submitTopology(
                        t.createTopologyName(),
                        t.createConfig(),
                        t.createTopology()
                ));
                break;

            // Esecuzione storm in modalità cluster
            case MODE_CLUSTER:
                config.getTopologies().forEach(t -> {
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

            // Errore imprevisto
            default:
                System.err.println("Errore sconosciuto");
                System.exit(config.getExitFailure());
        }
    }

}
