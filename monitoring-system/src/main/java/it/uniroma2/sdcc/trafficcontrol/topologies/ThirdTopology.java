package it.uniroma2.sdcc.trafficcontrol.topologies;

import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.CongestedSequencePublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.CongestionComputationWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.SequenceSelectorWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.boltsThirdQuery.SequencesDispatcherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoresSequencesManager;
import it.uniroma2.sdcc.trafficcontrol.entity.SequencesBolts;
import it.uniroma2.sdcc.trafficcontrol.spouts.KafkaSpout;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.CONGESTED_SEQUENCE;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.MOBILE_SENSOR_VALIDATED;
import static it.uniroma2.sdcc.trafficcontrol.constants.Params.Properties.ROAD_DELTA;
import static it.uniroma2.sdcc.trafficcontrol.constants.Params.Properties.SEMAPHORES_SEQUENCES_FILE;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class ThirdTopology extends Topology {

    private static final String CLASS_NAME = ThirdTopology.class.getSimpleName();

    @Override
    protected TopologyBuilder defineTopology() throws IllegalArgumentException {
        SequencesBolts sequencesBolts = new SequencesBolts(SEMAPHORES_SEQUENCES_FILE, ROAD_DELTA);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout(MOBILE_SENSOR_VALIDATED, CLASS_NAME),4);

        // Dispatcher che smista le varie tuple proveniente dai sensori mobili verso i bolts
        // relativi per il loro processamento (attraverso vari streams)
        builder.setBolt(SEQUENCES_DISPATCHER_BOLT, new SequencesDispatcherBolt(sequencesBolts),4)
                .shuffleGrouping(KAFKA_SPOUT);


        // Bolts che calcolano il grado di congestione
        sequencesBolts.getSequenceBoltList().forEach(
                sb -> builder.setBolt(
                        sb.getBoltName(),
                        new CongestionComputationWindowedBolt(5 * 60,4, sb.getSemaphoresSequence())
                )
                        .globalGrouping(SEQUENCES_DISPATCHER_BOLT, sb.getStreamName())
        );
        // Bolt che sceglie la sequenza di semafori più congestionata
        BoltDeclarer sequenceBoltDeclarer = builder.setBolt(
                SEQUENCE_SELECTOR_BOLT,
                new SequenceSelectorWindowedBolt(
                        5 * 60,
                        4,
                        SemaphoresSequencesManager.getsemaphoresSequenceFromBoltsList(sequencesBolts),
                        ROAD_DELTA
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
