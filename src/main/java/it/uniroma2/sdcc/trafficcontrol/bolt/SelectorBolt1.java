package it.uniroma2.sdcc.trafficcontrol.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.*;


public class SelectorBolt1 extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<Integer, Long> lampsMap;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.lampsMap = new HashMap<Integer, Long>();
    }

    public void execute(Tuple tuple) {
        Long timestamp = tuple.getLongByField(TIMESTAMP);

        Integer id = tuple.getIntegerByField(ID);
        String city = tuple.getStringByField(CITY);
        String address = tuple.getStringByField(ADDRESS);
        Integer km = tuple.getIntegerByField(KM);

        Boolean state = tuple.getBooleanByField(STATE);

        if (!lampsMap.containsKey(id) || lampsMap.get(id) < timestamp) {
            lampsMap.put(id, timestamp);
            Values values = new Values(id, city, address, km, state);
            collector.emit(values);
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID, CITY, ADDRESS, KM, STATE));
    }
}
