package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.constants.TupleFields;
import it.uniroma2.sdcc.trafficcontrol.utils.IntersectionItem;
import it.uniroma2.sdcc.trafficcontrol.utils.Ranking;
import it.uniroma2.sdcc.trafficcontrol.utils.TopKRanking;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.RANKING_DESTINATION;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.AVERAGE_VEHICLES_SPEED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.UPDATE_PARTIAL;
import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.RANK_ITEM;


public class GlobalRankBolt extends BaseRichBolt {
    /**
     * The GlobalRank does the merge of partial rankings.
     * In the case where the lamps whose bulbs needed replacement and for which a new bulb is installed
     * or in the case where a fault has occurred, we proceed to their elimination in the rankings.
     */
    private OutputCollector collector;
    private TopKRanking ranking;
    private int topK;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;


    public GlobalRankBolt(int topK) {
        this.topK = topK;
    }


    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;
        this.ranking = new TopKRanking(topK);
        this.mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA_IP_PORT);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple tuple) {
        boolean updated = false;

        if (tuple.getSourceStreamId().equals(UPDATE_PARTIAL)) {
            Ranking partialRanking = (Ranking) tuple.getValueByField(TupleFields.PARTIAL_RANK);
            /*Publish the ranking only if updates have occurred*/
            for (IntersectionItem item : partialRanking.getRanking()) {
                updated |= ranking.update(item);
            }

        } else {
            /*Delete from the list the streetlamps with broken lamps or lamps that no longer exceed the average life time*/
            IntersectionItem intersectionItem = (IntersectionItem) tuple.getValueByField(RANK_ITEM);
            /*
            Vcchio codice
            if (ranking.indexOf(intersectionItem) < topK)
                updated = true;
            ranking.remove(intersectionItem);
            */
            updated = ranking.remove(intersectionItem);
        }

        /* Emit if the local topK is changed */
        if (updated) {
            printRanking();
        } else {
            System.out.println(String.format("GLOBAL RANK\tupdated: %s\n", updated));
        }

        collector.ack(tuple);
    }

    private void printRanking() {
        List<IntersectionItem> globalTopK = ranking.getTopK().getRanking();
        /*globalTopK.forEach(e -> System.out.println(String.format(
                "INT: %d\tVEL: %d\n",
                e.getIntersectionId(),
                e.getAverageVehiclesSpeed()
        )));*/
        long currentTime = System.currentTimeMillis();

        for (IntersectionItem intersectionItem : globalTopK) {
            ObjectNode objectNode = mapper.createObjectNode();

            objectNode.put(INTERSECTION_ID, intersectionItem.getIntersectionId());
            objectNode.put(AVERAGE_VEHICLES_SPEED, intersectionItem.getAverageVehiclesSpeed());
            /*
            objectNode.put(CITY, intersectionItem.getCity());
            objectNode.put(ADDRESS, intersectionItem.getAddress());
            objectNode.put(KM, intersectionItem.getKm());
            objectNode.put(BULB_MODEL, intersectionItem.getModel());
            objectNode.put(TIME_DIFF, currentTime - (intersectionItem.getInstallationTimestamp() + intersectionItem.getMeanExpirationTime()));
            */

            producer.send(new ProducerRecord<>(RANKING_DESTINATION, objectNode.toString()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

}
