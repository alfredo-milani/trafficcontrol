package  it.uniroma2.sdcc.trafficcontrol.topology;

import it.uniroma2.sdcc.trafficcontrol.bolt.ParserBolt;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.GlobalRankBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.FilterBolt2;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.PartialRankBolt;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.SelectorBolt2;
import it.uniroma2.sdcc.trafficcontrol.spout.KafkaSpout;


import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.ID;


public class SecondTopology {

    private final TopologyBuilder builder;

    public SecondTopology() {
        this.builder = new TopologyBuilder();

        builder.setSpout(KAFKA_SPOUT, new KafkaSpout()).setNumTasks(4);
        builder.setBolt(PARSER_BOLT, new ParserBolt()).shuffleGrouping(KAFKA_SPOUT).setNumTasks(4);
        builder.setBolt(SELECTOR_BOLT_2, new SelectorBolt2()).fieldsGrouping(PARSER_BOLT, new Fields(ID)).setNumTasks(4);
        builder.setBolt(FILTER_BOLT_QUERY_2, new FilterBolt2()).fieldsGrouping(SELECTOR_BOLT_2, new Fields(ID)).setNumTasks(4);
        builder.setBolt(PARTIAL_RANK, new PartialRankBolt(10)).fieldsGrouping(FILTER_BOLT_QUERY_2, new Fields(ID)).setNumTasks(4);
        builder.setBolt(GLOBAL_RANK, new GlobalRankBolt(10), 1).allGrouping(PARTIAL_RANK, UPDATE).allGrouping(PARTIAL_RANK, REMOVE).setNumTasks(1);
    }

    public static TopologyBuilder setTopology(TopologyBuilder builder) {
        if (builder == null) {
            builder = new TopologyBuilder();
            builder.setSpout(KAFKA_SPOUT, new KafkaSpout()).setNumTasks(4);
            builder.setBolt(PARSER_BOLT, new ParserBolt()).localOrShuffleGrouping(KAFKA_SPOUT).setNumTasks(4);
        }
        builder.setBolt(SELECTOR_BOLT_2, new SelectorBolt2()).fieldsGrouping(PARSER_BOLT, new Fields(ID)).setNumTasks(4);
        builder.setBolt(FILTER_BOLT_QUERY_2, new FilterBolt2()).fieldsGrouping(SELECTOR_BOLT_2, new Fields(ID)).setNumTasks(4);
        builder.setBolt(PARTIAL_RANK, new PartialRankBolt(10)).fieldsGrouping(FILTER_BOLT_QUERY_2, new Fields(ID)).setNumTasks(4);
        builder.setBolt(GLOBAL_RANK, new GlobalRankBolt(10), 1).allGrouping(PARTIAL_RANK, UPDATE).allGrouping(PARTIAL_RANK, REMOVE).setNumTasks(1);
        return builder;
    }

    public StormTopology createTopology() {
        return builder.createTopology();
    }
}
