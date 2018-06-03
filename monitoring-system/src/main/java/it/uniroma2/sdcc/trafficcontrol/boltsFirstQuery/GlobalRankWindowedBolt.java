package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.utils.IntersectionItem;
import it.uniroma2.sdcc.trafficcontrol.utils.Ranking;
import it.uniroma2.sdcc.trafficcontrol.utils.TopKRanking;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.AVERAGE_VEHICLES_SPEED;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.UPDATE_PARTIAL;

public class GlobalRankWindowedBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private int d;
    private final int topK;
    private TopKRanking ranking;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;

    public GlobalRankWindowedBolt(int topK) {
        this.topK = topK;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.ranking = new TopKRanking(topK);
        this.mapper = new ObjectMapper();
        this.d = ThreadLocalRandom.current().nextInt(0, 100);


        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
        props.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        props.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        // System.out.println("--- INIZIO FINESTRA ID" + d + "----" + System.currentTimeMillis());
        List<Tuple> tuplesInWindow = tupleWindow.getNew();
        // tupleWindow.get().forEach(s -> System.out.println(String.format("G_BOLT: %d\tTUPLE: %d", d, s)));
        // tupleWindow.getNew().forEach(s -> System.out.println(String.format("G_BOLT: %d\tTUPLE NEW: %d", d, s)));
        // tupleWindow.getExpired().forEach(s -> System.out.println(String.format("G_BOLT: %d\tTUPLE EXP: %d", d, s)));

        boolean updated = false;
        for (Tuple tuple : tuplesInWindow) {
            Ranking partialRanking = (Ranking) tuple.getValueByField(UPDATE_PARTIAL);
            /*Publish the ranking only if updates have occurred*/
            for (IntersectionItem item : partialRanking.getRanking()) {
                updated |= ranking.update(item);
            }

            // superfluo perché le tuple expired sono ackate automaticamente
            collector.ack(tuple);
        }

        /* Emit if the local topK is changed */
        if (updated) {
            printRanking();
        } else {
            System.out.println(String.format("GLOBAL RANK\tupdated: %s\n", updated));
        }
    }

    private void printRanking() {
        List<IntersectionItem> globalTopK = ranking.getTopK().getRanking();
        globalTopK.forEach(e -> System.out.println(String.format(
                "BOLT: %d\tINT: %d\tVEL: %d",
                d,
                e.getIntersectionId(),
                e.getAverageVehiclesSpeed()
        )));
        System.out.println("\n");
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

    // TODO eliminare dalla classfica le tuple expired
}
