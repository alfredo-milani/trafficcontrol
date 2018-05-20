package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.topology.FirstTopology;
import org.apache.commons.cli.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.*;


public class TopologyStarter {

    public static void main(String[] args) {
        /*
        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        KAFKA_IP_PORT = "localhost:9092";

        SecondTopology secondTopology = new SecondTopology();

        localCluster.submitTopology("topologyName", conf, secondTopology.setTopology().createTopology());
        */


        parseArgs(args);

        // TODO METTERE INIZIALIZZAZIONE TOPOLOGIA (PARTE COMUNE IN TUTTE LE TOPOLOGIE... VEDERE SE builder == null è uguale a tutte)
        // TODO     IN UNA CLASSE A PARTE
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();

        FirstTopology firstTopology = new FirstTopology();
        // SecondTopology secondTopology = new SecondTopology();
        // ThirdTopology thirdTopology = new ThirdTopology();
        // conf.setNumWorkers(6);

        /*
         * Apache Storm processes, called workers, run on predefined ports on the machine that hosts Storm.
         *
         * Each worker process can run one or more executors, or threads, where each executor is a thread spawned by the worker process.
         *
         * Each executor runs one or more tasks from the same component, where a component is a spout or bolt from a topology.
         *
         *
         *
         * In storm the term parallelism hint is used to specify the initial number of executor (threads) of a component (spout, bolt) (this can be changed in the run time)
         *
         * the setNumTasks(4) indicate to run 4 associated tasks (this will be same throughout the lifetime of a topology).
         * So in this case each storm will be running two tasks per executor.
         * By default, the number of tasks is set to be the same as the number of executors, i.e. Storm will run one task per thread.
         *
         */

        switch (MODE_SELECTED) {
            case MODE_LOCAL:
                cluster.submitTopology(
                        "LocalTopology",
                        conf,
                        firstTopology.setLocalTopology().createTopology()
                );
                break;

            case MODE_CLUSTER:
                try {
                    StormSubmitter.submitTopology(
                            "RemoteTopology",
                            conf,
                            firstTopology.setRemoteTopology().createTopology()
                    );
                } catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
                    e.printStackTrace();
                }
                break;

            default:
                System.err.println(String.format(
                        "Argomento non valido: %s.",
                        MODE_SELECTED
                ));
        }
    }

    private static void parseArgs(String[] args) {
        Options options = new Options();

        Option mode = new Option(
                MODE,
                String.format("%s=", MODE_LONG),
                true,
                "Tipologia deploy: local o cluster"
        );
        mode.setRequired(false);
        options.addOption(mode);

        Option kafkaIp = new Option(
                KAFKA_IP,
                String.format("%s=", KAFKA_IP_LONG),
                true,
                "Hostname del server su cui è presente un'istanza di Kafka"
        );
        kafkaIp.setRequired(true);
        options.addOption(kafkaIp);

        Option kafkaPort = new Option(
                KAFKA_PORT,
                String.format("%s=", KAFKA_PORT_LONG),
                true,
                String.format("Porta su cui è in ascolto l'istanza di Kafka (default: %s)", KAFKA_IP_PORT)
        );
        kafkaPort.setRequired(false);
        options.addOption(kafkaPort);

        Option numberWorkers = new Option(
                NUMBER_WORKERS,
                String.format("%s=", NUMBER_WORKERS_LONG),
                true,
                String.format("Numero di workers nella topologia (default %d)", NUMBER_WORKERS_DEFAULT)
        );
        numberWorkers.setRequired(false);
        options.addOption(numberWorkers);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp(
                    String.format(
                            "<%s> -%s [local | cluster] -%s [IP address] -%s [port number]",
                            APP_NAME,
                            MODE,
                            KAFKA_IP,
                            KAFKA_PORT
                    ),
                    options
            );

            System.exit(EXIT_FAILURE);
            return;
        }

        MODE_SELECTED = cmd.getOptionValue(MODE) == null ? MODE_SELECTED : cmd.getOptionValue(MODE);
        KAFKA_IP_SELECTED = cmd.getOptionValue(KAFKA_IP);
        KAFKA_PORT_SELECTED = cmd.getOptionValue(KAFKA_PORT) == null ? KAFKA_PORT_SELECTED : cmd.getOptionValue(KAFKA_PORT);

        KAFKA_IP_PORT = String.format(
                "%s:%s",
                KAFKA_IP_SELECTED,
                KAFKA_PORT_SELECTED
        );
    }

}
