package it.uniroma2.sdcc.trafficcontrol.bolt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jdk.nashorn.internal.ir.ObjectNode;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;



import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.*;

public class ParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    public void execute(Tuple tuple) {
        try {
            String raw_tuple = tuple.getStringByField(RAW_TUPLE);
            JsonNode jsonNode = mapper.readTree(raw_tuple);

            Long timestamp = tuple.getLongByField(TIMESTAMP);
            Integer id = jsonNode.get(ID).asInt();
            String city = jsonNode.get(CITY).asText();
            String address = jsonNode.get(ADDRESS).asText();
            Integer km = jsonNode.get(KM).asInt();
            String bulbModel = jsonNode.get(BULB).get(BULB_MODEL).asText();
            Double maxWatts = jsonNode.get(BULB).get(MAX_WATTS).asDouble();
            Double currentWatts = jsonNode.get(BULB).get(CURRENT_WATTS).asDouble();
            Long installationTimestamp = jsonNode.get(BULB).get(INSTALLATION_TIMESTAMP).asLong();
            Long meanExpirationTime = jsonNode.get(BULB).get(MEAN_EXPIRATION_TIME).asLong();
            Boolean state = jsonNode.get(BULB).get(STATE).asBoolean();

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
        declarer.declare(new Fields(TIMESTAMP, ID, CITY, ADDRESS, KM, BULB_MODEL, MAX_WATTS, CURRENT_WATTS,
                INSTALLATION_TIMESTAMP, MEAN_EXPIRATION_TIME, STATE));
    }
}