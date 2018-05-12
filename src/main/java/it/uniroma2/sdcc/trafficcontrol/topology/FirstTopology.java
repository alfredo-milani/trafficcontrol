package it.uniroma2.sdcc.trafficcontrol.topology;

import it.uniroma2.sdcc.trafficcontrol.bolt.FilterBolt1;
import it.uniroma2.sdcc.trafficcontrol.bolt.ParserBolt;
import it.uniroma2.sdcc.trafficcontrol.bolt.SelectorBolt1;
import it.uniroma2.sdcc.trafficcontrol.spout.KafkaSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.ID;

public class FirstTopology extends Topology {

    public FirstTopology() {
        super();
    }

    @Override
    public StormTopology createTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout());
        builder.setBolt(PARSER_BOLT, new ParserBolt()).shuffleGrouping(KAFKA_SPOUT);
        builder.setBolt(SELECTOR_BOLT, new SelectorBolt1()).fieldsGrouping(PARSER_BOLT, new Fields(ID));
        builder.setBolt(FILTER_BOLT_QUERY_1, new FilterBolt1()).fieldsGrouping(SELECTOR_BOLT, new Fields(ID));

        return builder.createTopology();
    }
}