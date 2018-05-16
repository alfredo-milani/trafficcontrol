package it.uniroma2.sdcc.trafficcontrol.topology;

import it.uniroma2.sdcc.trafficcontrol.bolt.ParserBolt;
import it.uniroma2.sdcc.trafficcontrol.constants.StormParams;
import it.uniroma2.sdcc.trafficcontrol.constants.TupleFields;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.FilterBolt2;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.GlobalRankBolt;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.PartialRankBolt;
import it.uniroma2.sdcc.trafficcontrol.firstquerybolts.SelectorBolt2;
import it.uniroma2.sdcc.trafficcontrol.spout.KafkaSpout;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


public class SecondTopology {

    private final TopologyBuilder builder;

    public SecondTopology() {
        this.builder = new TopologyBuilder();

        builder.setSpout(StormParams.KAFKA_SPOUT, new KafkaSpout()).setNumTasks(4);
        builder.setBolt(StormParams.PARSER_BOLT, new ParserBolt()).shuffleGrouping(StormParams.KAFKA_SPOUT).setNumTasks(4);
        builder.setBolt(StormParams.SELECTOR_BOLT_2, new SelectorBolt2()).fieldsGrouping(StormParams.PARSER_BOLT, new Fields(TupleFields.ID)).setNumTasks(4);
        builder.setBolt(StormParams.FILTER_BOLT_QUERY_2, new FilterBolt2()).fieldsGrouping(StormParams.SELECTOR_BOLT_2, new Fields(TupleFields.ID)).setNumTasks(4);
        builder.setBolt(StormParams.PARTIAL_RANK, new PartialRankBolt(10)).fieldsGrouping(StormParams.FILTER_BOLT_QUERY_2, new Fields(TupleFields.ID)).setNumTasks(4);
        builder.setBolt(StormParams.GLOBAL_RANK, new GlobalRankBolt(10), 1).allGrouping(StormParams.PARTIAL_RANK, StormParams.UPDATE).allGrouping(StormParams.PARTIAL_RANK, StormParams.REMOVE).setNumTasks(1);
    }

    public static TopologyBuilder setTopology(TopologyBuilder builder) {
        if (builder == null) {
            builder = new TopologyBuilder();
            builder.setSpout(StormParams.KAFKA_SPOUT, new KafkaSpout()).setNumTasks(4);
            builder.setBolt(StormParams.PARSER_BOLT, new ParserBolt()).localOrShuffleGrouping(StormParams.KAFKA_SPOUT).setNumTasks(4);
        }
        builder.setBolt(StormParams.SELECTOR_BOLT_2, new SelectorBolt2()).fieldsGrouping(StormParams.PARSER_BOLT, new Fields(TupleFields.ID)).setNumTasks(4);
        builder.setBolt(StormParams.FILTER_BOLT_QUERY_2, new FilterBolt2()).fieldsGrouping(StormParams.SELECTOR_BOLT_2, new Fields(TupleFields.ID)).setNumTasks(4);
        builder.setBolt(StormParams.PARTIAL_RANK, new PartialRankBolt(10)).fieldsGrouping(StormParams.FILTER_BOLT_QUERY_2, new Fields(TupleFields.ID)).setNumTasks(4);
        builder.setBolt(StormParams.GLOBAL_RANK, new GlobalRankBolt(10), 1).allGrouping(StormParams.PARTIAL_RANK, StormParams.UPDATE).allGrouping(StormParams.PARTIAL_RANK, StormParams.REMOVE).setNumTasks(1);
        return builder;
    }

    public StormTopology createTopology() {
        return builder.createTopology();
    }
}
