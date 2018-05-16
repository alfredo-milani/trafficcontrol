package it.uniroma2.sdcc.trafficcontrol.firstquerybolts;

import it.uniroma2.sdcc.trafficcontrol.constants.TupleFields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;


public class FilterBolt2 extends BaseRichBolt {
    /**
     * The FilterBolt select the information relating to streetlamps for which there has been a change of state.
     * By "state" we mean "the bulb installed needs or not need to be replaced because it has or has not exceeded the average life time" .
     * FilterBolt2 must communicate also the streetlamps with defective bulb because they must be excluded from the ranking.
     */
    private OutputCollector collector;
    private HashMap<Integer, Boolean> lampsMap;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.lampsMap = new HashMap<Integer, Boolean>(); // key: id of streetlamps, key: true if bulb needs to be replaced
    }

    public void execute(Tuple tuple) {

        Integer id = tuple.getIntegerByField(TupleFields.ID);
        String city = tuple.getStringByField(TupleFields.CITY);
        String address = tuple.getStringByField(TupleFields.ADDRESS);
        Integer km = tuple.getIntegerByField(TupleFields.KM);
        String model = tuple.getStringByField(TupleFields.BULB_MODEL);
        Long installationTimestamp = tuple.getLongByField(TupleFields.INSTALLATION_TIMESTAMP);
        Long meanExpirationTime = tuple.getLongByField(TupleFields.MEAN_EXPIRATION_TIME);
        Boolean state = tuple.getBooleanByField(TupleFields.STATE); // false if defective bulb

        Long currentTime = System.currentTimeMillis();

        Boolean shouldBeInRank; //true: bulb needs to be replaced
        Boolean needUpdate = false; // true: change of state

        //Checking if the bulb has failed or needs replacing
        shouldBeInRank = state && currentTime - installationTimestamp >= meanExpirationTime;

        if (!lampsMap.containsKey(id))
            lampsMap.put(id, shouldBeInRank);

        if (lampsMap.get(id) != shouldBeInRank || lampsMap.get(id)) { //check if there is a change of state
            lampsMap.put(id, shouldBeInRank);
            needUpdate = true;
        }

        if (needUpdate) {
            // Enter tuple because there has been a change of state
            Values values = new Values(id, city, address, km, model, installationTimestamp, meanExpirationTime, shouldBeInRank);
            collector.emit(values);
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TupleFields.ID, TupleFields.CITY, TupleFields.ADDRESS, TupleFields.KM, TupleFields.BULB_MODEL, TupleFields.INSTALLATION_TIMESTAMP,
                TupleFields.MEAN_EXPIRATION_TIME, TupleFields.SHOULD_BE_IN_RANK));
    }
}
