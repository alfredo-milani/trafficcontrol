package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.CongestedSequencePublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.CongestionComputationWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.SequenceSelectorWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.SequencesDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SemaphoresSequencesManager;
import it.uniroma2.sdcc.trafficcontrol.entity.thirdQuery.SequencesBolts;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.CONGESTED_SEQUENCE;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.MOBILE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;
import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.ROAD_DELTA;
import static it.uniroma2.sdcc.trafficcontrol.entity.configuration.Config.SEMAPHORES_SEQUENCES_FILE;

public class ThirdTopology extends Topology {

    private static final String CLASS_NAME = ThirdTopology.class.getSimpleName();
    // File di configurazione onfigurazione
    private final static Config config;
    static {
        config = Config.getInstance();
        try {
            // Caricamento proprietà
            config.loadIfHasNotAlreadyBeenLoaded();
        } catch (IOException e) {
            System.err.println(String.format(
                    "%s: error while reading configuration file",
                    ThirdTopology.class.getSimpleName()
            ));
            e.printStackTrace();
        }
    }

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        TopologyBuilder builder = new TopologyBuilder();

        SequencesBolts sequencesBolts = new SequencesBolts(
                (String) config.get(SEMAPHORES_SEQUENCES_FILE),
                (double) config.get(ROAD_DELTA)
        );
        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(MOBILE_SENSOR_VALIDATED, CLASS_NAME),4);

        // Dispatcher che smista le varie tuple proveniente dai sensori mobili verso i abstractsBolts
        // relativi per il loro processamento (attraverso vari streams)
        builder.setBolt(SEQUENCES_DISPATCHER_BOLT, new SequencesDispatcherBolt(sequencesBolts),4)
                .shuffleGrouping(KAFKA_SPOUT);


        // Bolts che calcolano il grado di congestione.
        // Viene creato un bolt per ogni sequenza di semafori da controllare
        sequencesBolts.getSequenceBoltList().forEach(
                sb -> builder.setBolt(
                        sb.getBoltName(),
                        new CongestionComputationWindowedBolt(
                                TimeUnit.MINUTES.toSeconds(5),
                                TimeUnit.SECONDS.toSeconds(5),
                                sb.getSemaphoresSequence()
                        )
                )
                        .globalGrouping(SEQUENCES_DISPATCHER_BOLT, sb.getStreamName())
        );
        // Bolt che sceglie la sequenza di semafori più congestionata
        BoltDeclarer sequenceBoltDeclarer = builder.setBolt(
                SEQUENCE_SELECTOR_BOLT,
                new SequenceSelectorWindowedBolt(
                        TimeUnit.MINUTES.toSeconds(5),
                        TimeUnit.SECONDS.toSeconds(5),
                        SemaphoresSequencesManager.getsemaphoresSequenceFromBoltsList(sequencesBolts),
                        (double) config.get(ROAD_DELTA)
                )
        );
        sequencesBolts.getSequenceBoltList().forEach(
                sb -> sequenceBoltDeclarer.globalGrouping(sb.getBoltName())
        );


        // Publisher bolt per la pubblicazione della sequenza più congestionata
        builder.setBolt(CONGESTED_SEQUENCE_PUBLISHER_BOLT, new CongestedSequencePublisherBolt(CONGESTED_SEQUENCE),2)
                .shuffleGrouping(SEQUENCE_SELECTOR_BOLT);

        return builder;
    }

    @Override
    protected String defineTopologyName() {
        return CLASS_NAME;
    }

}
