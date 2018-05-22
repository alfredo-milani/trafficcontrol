package it.uniroma2.sdcc.trafficcontrol.firstQueryBolts;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.topology.FirstTopology;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_STATUS;

public class SemaphoreStatusBolt extends BaseRichBolt {

    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(SERVER, KAFKA_IP_PORT);
        props.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        props.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple tuple) {
        Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
        Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
        Long semaphoreTimestampUTC = tuple.getLongByField(SEMAPHORE_TIMESTAMP_UTC);
        Short greenLightDuration = tuple.getShortByField(GREEN_LIGHT_DURATION);
        Byte greenLightStatus = tuple.getByteByField(GREEN_LIGHT_STATUS);
        Byte yellowLightStatus = tuple.getByteByField(YELLOW_LIGHT_STATUS);
        Byte redLightStatus = tuple.getByteByField(RED_LIGHT_STATUS);
        Short vehiclesPerSecond = tuple.getShortByField(VEHICLES_PER_SECOND);
        Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);

        FirstTopology.getLOGGER().log(Level.INFO, semaphoreId.toString());

        Boolean isSemaphoreBroken = true;
        if (greenLightStatus < LAMP_CODE_TWO_THIRD ||
                yellowLightStatus < LAMP_CODE_TWO_THIRD ||
                redLightStatus < LAMP_CODE_OK) {
            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put(INTERSECTION_ID, intersectionId);
            objectNode.put(SEMAPHORE_ID, semaphoreId);
            objectNode.put(SEMAPHORE_LATITUDE, semaphoreLatitude);
            objectNode.put(SEMAPHORE_LONGITUDE, semaphoreLongitude);

            String greenStatus;
            if (greenLightStatus >= LAMP_CODE_FAULTY && greenLightStatus < LAMP_CODE_ONE_THIRD) {
                isSemaphoreBroken = false;
                greenStatus = LAMP_STATUS_FAULTY;
            } else if (greenLightStatus >= LAMP_CODE_ONE_THIRD && greenLightStatus < LAMP_CODE_TWO_THIRD) {
                greenStatus = LAMP_STATUS_AVERAGE;
            } else {
                greenStatus = LAMP_STATUS_OK;
            }
            objectNode.put(GREEN_LIGHT_STATUS, greenStatus);

            String yellowStatus;
            if (yellowLightStatus >= LAMP_CODE_FAULTY && yellowLightStatus < LAMP_CODE_ONE_THIRD) {
                isSemaphoreBroken = false;
                yellowStatus = LAMP_STATUS_FAULTY;
            } else if (yellowLightStatus >= LAMP_CODE_ONE_THIRD && yellowLightStatus < LAMP_CODE_TWO_THIRD) {
                yellowStatus = LAMP_STATUS_AVERAGE;
            } else {
                yellowStatus = LAMP_STATUS_OK;
            }
            objectNode.put(YELLOW_LIGHT_STATUS, yellowStatus);

            String redStatus;
            if (redLightStatus >= LAMP_CODE_FAULTY && redLightStatus < LAMP_CODE_ONE_THIRD) {
                isSemaphoreBroken = false;
                redStatus = LAMP_STATUS_FAULTY;
            } else if (redLightStatus >= LAMP_CODE_ONE_THIRD && redLightStatus < LAMP_CODE_TWO_THIRD) {
                redStatus = LAMP_STATUS_AVERAGE;
            } else {
                redStatus = LAMP_STATUS_OK;
            }
            objectNode.put(RED_LIGHT_STATUS, redStatus);
            /*
            objectNode.put(
                    GREEN_LIGHT_STATUS,
                    LAMP_STATUS_CODE.get(greenLightStatus) == null ?
                            LAMP_STATUS_OK : LAMP_STATUS_CODE.get(greenLightStatus)
            );
            objectNode.put(
                    YELLOW_LIGHT_STATUS,
                    LAMP_STATUS_CODE.get(yellowLightStatus) == null ?
                            LAMP_STATUS_OK : LAMP_STATUS_CODE.get(yellowLightStatus)
            );
            objectNode.put(
                    RED_LIGHT_STATUS,
                    LAMP_STATUS_CODE.get(redLightStatus) == null ?
                            LAMP_STATUS_OK : LAMP_STATUS_CODE.get(redLightStatus)
            );
            */

            producer.send(new ProducerRecord<>(SEMAPHORE_STATUS, objectNode.toString()));
        }

        Values values = new Values(
                intersectionId,
                semaphoreId,
                semaphoreLatitude,
                semaphoreLongitude,
                semaphoreTimestampUTC,
                greenLightDuration,
                isSemaphoreBroken,
                vehiclesPerSecond,
                averageVehiclesSpeed
        );

        collector.emit(values);

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                SEMAPHORE_ID,
                SEMAPHORE_LATITUDE,
                SEMAPHORE_LONGITUDE,
                SEMAPHORE_TIMESTAMP_UTC,
                GREEN_LIGHT_DURATION,
                SEMAPHORE_STATUS,
                VEHICLES_PER_SECOND,
                AVERAGE_VEHICLES_SPEED
        ));
    }

}
