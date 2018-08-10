package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.topologies.*;
import lombok.Cleanup;
import lombok.extern.java.Log;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import static it.uniroma2.sdcc.trafficcontrol.constants.Params.EXIT_FAILURE;
import static it.uniroma2.sdcc.trafficcontrol.constants.Params.Properties.*;


@Log
public class TopologyStarter {

    public static void main(String[] args) throws IOException {
        fillProperties();

        List<Topology> topologies = Lists.newArrayList(
                new ValidationTopology(),
                //new SemaphoreStatusTopology(),
                //new FirstTopology()
                new SecondTopology()
                //new ThirdTopology()
                //new GreenSettingTopology()
        );

        switch (MODE) {
            case MODE_LOCAL:
                final LocalCluster cluster = new LocalCluster();

                topologies.forEach(t -> cluster.submitTopology(
                        t.createTopologyName(),
                        t.createConfig(),
                        t.createTopology()
                ));
                break;

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
                log.log(Level.SEVERE, "Errore sconosciuto");
                System.exit(EXIT_FAILURE);
        }
    }

    private static void fillProperties()
            throws IOException {
        Properties properties = new Properties();

        @Cleanup InputStream input = TopologyStarter.class
                .getClassLoader()
                .getResourceAsStream(PROPERTIES_FILENAME);

        if (input == null) {
            System.out.println("Sorry, unable to find " + PROPERTIES_FILENAME);
            return;
        }

        properties.load(input);

        KAFKA_IP = properties.getProperty(P_KAFKA_IP);
        KAFKA_PORT = properties.getProperty(P_KAFKA_PORT) == null
                ? KAFKA_PORT_DEFAULT
                : Integer.valueOf(properties.getProperty(P_KAFKA_PORT));
        KAFKA_IP_PORT = String.format("%s:%d", KAFKA_IP, KAFKA_PORT);
        NUMBER_OF_WORKERS = properties.getProperty(P_NUMBER_OF_WORKERS) == null
                ? NUMBER_WORKERS_DEFAULT
                : Integer.valueOf(properties.getProperty(P_NUMBER_OF_WORKERS));
    }

}
