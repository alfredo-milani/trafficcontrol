package it.uniroma2.sdcc.trafficcontrol.firstquerybolts;

import it.uniroma2.sdcc.trafficcontrol.constants.TupleFields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class SelectorBolt2 extends BaseRichBolt {
    /**
     * The SelectorBolt2 selects the information needed to determine
     * the ranking of the top k streetlamps whose bulbs have exceeded the average life time estimated.
     */
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {

        Integer id = tuple.getIntegerByField(TupleFields.ID);
        String city = tuple.getStringByField(TupleFields.CITY);
        String address = tuple.getStringByField(TupleFields.ADDRESS);
        Integer km = tuple.getIntegerByField(TupleFields.KM);
        String model = tuple.getStringByField(TupleFields.BULB_MODEL);
        Long installationTimestamp = tuple.getLongByField(TupleFields.INSTALLATION_TIMESTAMP);
        Long meanExpirationTime = tuple.getLongByField(TupleFields.MEAN_EXPIRATION_TIME);
        Boolean state = tuple.getBooleanByField(TupleFields.STATE); //The streetlamps with lamp failure must be removed from the rankings

        Values values = new Values(id, city, address, km, model, installationTimestamp, meanExpirationTime, state);
        collector.emit(values);
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TupleFields.ID, TupleFields.CITY, TupleFields.ADDRESS, TupleFields.KM, TupleFields.BULB_MODEL, TupleFields.INSTALLATION_TIMESTAMP,
                TupleFields.MEAN_EXPIRATION_TIME, TupleFields.STATE));
    }
}
