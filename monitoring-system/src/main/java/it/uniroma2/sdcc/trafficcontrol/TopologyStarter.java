package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.exceptions.WrongCommandLineArgument;
import it.uniroma2.sdcc.trafficcontrol.topologies.*;
import org.apache.commons.cli.*;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.*;


public class TopologyStarter {

    private final static String CLASS_NAME = TopologyStarter.class.getName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    public static void main(String[] args) {
        parseArgs(args);

        List<Topology> topologies = Lists.newArrayList(
                new ValidationTopology(),
                new SemaphoreStatusTopology(),
                new FirstTopology(),
                new SecondTopology(),
                // new ThirdTopology(),
                new GreenSettingTopology()
        );

        switch (MODE_SELECTED) {
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
                LOGGER.log(Level.SEVERE, "Errore sconosciuto");
                System.exit(EXIT_FAILURE);
        }
    }

    private static void parseArgs(String[] args) throws WrongCommandLineArgument {
        Options options = new Options();

        Option mode = new Option(
                MODE,
                String.format("%s=", MODE_LONG),
                true,
                "Tipologia deploy: local o cluster (default: local)"
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
                String.format("Porta su cui è in ascolto l'istanza di Kafka (default: %s)", KAFKA_PORT_SELECTED)
        );
        kafkaPort.setRequired(false);
        options.addOption(kafkaPort);

        Option numberWorkers = new Option(
                NUMBER_WORKERS,
                String.format("%s=", NUMBER_WORKERS_LONG),
                true,
                String.format("Numero di workers nella topologia (default: %d)", NUMBER_WORKERS_SELECTED)
        );
        numberWorkers.setRequired(false);
        options.addOption(numberWorkers);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            LOGGER.log(Level.CONFIG, e.getMessage());
            formatter.printHelp(
                    String.format(
                            "<%s> -%s [local | cluster] -%s [IP address] -%s [port number] -%s [num of workers]",
                            APP_NAME,
                            MODE,
                            KAFKA_IP,
                            KAFKA_PORT,
                            NUMBER_WORKERS
                    ),
                    options
            );

            System.exit(EXIT_FAILURE);
            return;
        }

        // Validità argomenti
        String modeResult = cmd.getOptionValue(MODE) == null ?
                MODE_SELECTED : cmd.getOptionValue(MODE);
        if (!modeResult.equals(MODE_LOCAL) && !modeResult.equals(MODE_CLUSTER)) {
            throw new WrongCommandLineArgument(String.format(
                    "Argument '%s' must be '%s' or '%s'. Current value: '%s'.",
                    MODE,
                    MODE_LOCAL,
                    MODE_CLUSTER,
                    modeResult
            ));
        }
        MODE_SELECTED = modeResult;

        try {
            KAFKA_PORT_SELECTED = cmd.getOptionValue(KAFKA_PORT) == null ?
                    KAFKA_PORT_SELECTED : Integer.valueOf(cmd.getOptionValue(KAFKA_PORT));
        } catch (NumberFormatException e) {
            throw new WrongCommandLineArgument(String.format(
                    "Argument '%s' must be integer. Current value: '%s'.",
                    KAFKA_PORT,
                    cmd.getOptionValue(KAFKA_PORT)
            ));
        }

        try {
            NUMBER_WORKERS_SELECTED = cmd.getOptionValue(NUMBER_WORKERS) == null ?
                    NUMBER_WORKERS_SELECTED : Integer.valueOf(cmd.getOptionValue(NUMBER_WORKERS));
        } catch (NumberFormatException e) {
            throw new WrongCommandLineArgument(String.format(
                    "Argument '%s' must be integer. Current value: '%s'.",
                    NUMBER_WORKERS,
                    cmd.getOptionValue(NUMBER_WORKERS)
            ));
        }

        KAFKA_IP_SELECTED = cmd.getOptionValue(KAFKA_IP);

        KAFKA_IP_PORT = String.format(
                "%s:%d",
                KAFKA_IP_SELECTED,
                KAFKA_PORT_SELECTED
        );
    }

    public Logger getLOGGER() {
        return LOGGER;
    }

}
