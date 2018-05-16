package it.uniroma2.sdcc.trafficcontrol.bolt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.constants.TupleFields;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class ParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    public void execute(Tuple tuple) {
        try {
            String raw_tuple = tuple.getStringByField(TupleFields.RAW_TUPLE);
            JsonNode jsonNode = mapper.readTree(raw_tuple);

            Long timestamp = tuple.getLongByField(TupleFields.TIMESTAMP);
            Integer id = jsonNode.get(TupleFields.ID).asInt();
            String city = jsonNode.get(TupleFields.CITY).asText();
            String address = jsonNode.get(TupleFields.ADDRESS).asText();
            Integer km = jsonNode.get(TupleFields.KM).asInt();
            String bulbModel = jsonNode.get(TupleFields.BULB).get(TupleFields.BULB_MODEL).asText();
            Double maxWatts = jsonNode.get(TupleFields.BULB).get(TupleFields.MAX_WATTS).asDouble();
            Double currentWatts = jsonNode.get(TupleFields.BULB).get(TupleFields.CURRENT_WATTS).asDouble();
            Long installationTimestamp = jsonNode.get(TupleFields.BULB).get(TupleFields.INSTALLATION_TIMESTAMP).asLong();
            Long meanExpirationTime = jsonNode.get(TupleFields.BULB).get(TupleFields.MEAN_EXPIRATION_TIME).asLong();
            Boolean state = jsonNode.get(TupleFields.BULB).get(TupleFields.STATE).asBoolean();

            Values values = new Values(timestamp, id, city, address, km, bulbModel, maxWatts, currentWatts,
                    installationTimestamp, meanExpirationTime, state);

            System.out.println(values);

            collector.emit(values);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TupleFields.TIMESTAMP, TupleFields.ID, TupleFields.CITY, TupleFields.ADDRESS, TupleFields.KM, TupleFields.BULB_MODEL, TupleFields.MAX_WATTS, TupleFields.CURRENT_WATTS,
                TupleFields.INSTALLATION_TIMESTAMP, TupleFields.MEAN_EXPIRATION_TIME, TupleFields.STATE));
    }
}