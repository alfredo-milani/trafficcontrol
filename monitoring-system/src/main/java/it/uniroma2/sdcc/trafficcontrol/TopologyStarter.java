package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.io.IOException;

import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig.MODE_CLUSTER;
import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig.MODE_LOCAL;

public class TopologyStarter {

    // File di configurazione
    private final static AppConfig appConfig = AppConfig.getInstance();

    public static void main(String[] args)
            throws IOException {
        // Controllo se è stato passato un file di configurazione perSi deve specificare un endpoint valido
        // l'applicazione da linea di comando
        if (args.length != 0) appConfig.load(args[0]);
        else appConfig.load();

        // Stampa configurazione attuale
        System.out.println(appConfig.toString());

        switch (appConfig.getMode()) {
            // Esecuzione storm in modalità locale
            case MODE_LOCAL:
                final LocalCluster cluster = new LocalCluster();

                appConfig.getTopologies().forEach(t -> cluster.submitTopology(
                        t.createTopologyName(),
                        t.createConfig(),
                        t.createTopology()
                ));
                break;

            // Esecuzione storm in modalità cluster
            case MODE_CLUSTER:
                appConfig.getTopologies().forEach(t -> {
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
                System.exit(appConfig.getExitFailure());
        }
    }

}
