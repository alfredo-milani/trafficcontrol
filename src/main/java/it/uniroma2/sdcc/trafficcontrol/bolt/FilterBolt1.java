package it.uniroma2.sdcc.trafficcontrol.bolt;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.PUBLISH_TOPIC;
import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.*;


public class FilterBolt1 extends BaseRichBolt {

    private Map<Integer, Boolean> lampState;
    private OutputCollector collector;
    private KafkaProducer<String, String> producer;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.lampState = new HashMap<Integer, Boolean>();

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_IP_PORT);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<String, String>(props);
    }

    public void execute(Tuple tuple) {
        Integer id = tuple.getIntegerByField(ID);
        String city = tuple.getStringByField(CITY);
        String address = tuple.getStringByField(ADDRESS);
        Integer km = tuple.getIntegerByField(KM);
        Boolean state = tuple.getBooleanByField(STATE);

        Boolean needUpdate = false;

        if (!lampState.containsKey(id)) {
            lampState.put(id, state);
            needUpdate = true;
        } else if (lampState.get(id) ^ state) { //XOR
            lampState.put(id, state);
            needUpdate = true;
        }

        collector.ack(tuple);

        /*
        ObjectMapper mapper = new ObjectMapper();
        try {
            String jsonInString = mapper.writeValueAsString(tuple);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        */

        if (needUpdate) {
            //TODO controlla stringa da mandare
            String toSend = "Lamp{" +
                    "id=" + id +
                    ", city='" + city + "\', address=\'" + address + "\', km=" + km
                    + ", state='" + state + '\'' +
                    "}";
            producer.send(new ProducerRecord<String, String>(PUBLISH_TOPIC, toSend));
            System.out.println(toSend);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
