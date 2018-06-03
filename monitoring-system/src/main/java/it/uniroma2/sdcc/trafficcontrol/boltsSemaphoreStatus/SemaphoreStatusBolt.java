package it.uniroma2.sdcc.trafficcontrol.boltsSemaphoreStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_RAW_TUPLE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;

public class SemaphoreStatusBolt extends BaseRichBolt {

    private OutputCollector collector;
    private ObjectMapper mapper;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String rawTuple = tuple.getStringByField(KAFKA_RAW_TUPLE);
            JsonNode jsonNode = mapper.readTree(rawTuple);

            Long intersectionId = jsonNode.get(INTERSECTION_ID).asLong();
            Long semaphoreId = jsonNode.get(SEMAPHORE_ID).asLong();
            Double semaphoreLatitude = jsonNode.get(SEMAPHORE_LATITUDE).asDouble();
            Double semaphoreLongitude = jsonNode.get(SEMAPHORE_LONGITUDE).asDouble();
            Long semaphoreTimestampUTC = jsonNode.get(SEMAPHORE_TIMESTAMP_UTC).asLong();
            Byte greenLightStatus = (byte) jsonNode.get(GREEN_LIGHT_STATUS).asInt();
            Byte yellowLightStatus = (byte) jsonNode.get(YELLOW_LIGHT_STATUS).asInt();
            Byte redLightStatus = (byte) jsonNode.get(RED_LIGHT_STATUS).asInt();

            if (greenLightStatus < LAMP_CODE_TWO_THIRD ||
                    yellowLightStatus < LAMP_CODE_TWO_THIRD ||
                    redLightStatus < LAMP_CODE_OK) {
                String greenStatus;
                if (greenLightStatus >= LAMP_CODE_FAULTY && greenLightStatus < LAMP_CODE_ONE_THIRD) {
                    greenStatus = LAMP_STATUS_FAULTY;
                } else if (greenLightStatus >= LAMP_CODE_ONE_THIRD && greenLightStatus < LAMP_CODE_TWO_THIRD) {
                    greenStatus = LAMP_STATUS_AVERAGE;
                } else {
                    greenStatus = LAMP_STATUS_OK;
                }

                String yellowStatus;
                if (yellowLightStatus >= LAMP_CODE_FAULTY && yellowLightStatus < LAMP_CODE_ONE_THIRD) {
                    yellowStatus = LAMP_STATUS_FAULTY;
                } else if (yellowLightStatus >= LAMP_CODE_ONE_THIRD && yellowLightStatus < LAMP_CODE_TWO_THIRD) {
                    yellowStatus = LAMP_STATUS_AVERAGE;
                } else {
                    yellowStatus = LAMP_STATUS_OK;
                }

                String redStatus;
                if (redLightStatus >= LAMP_CODE_FAULTY && redLightStatus < LAMP_CODE_ONE_THIRD) {
                    redStatus = LAMP_STATUS_FAULTY;
                } else if (redLightStatus >= LAMP_CODE_ONE_THIRD && redLightStatus < LAMP_CODE_TWO_THIRD) {
                    redStatus = LAMP_STATUS_AVERAGE;
                } else {
                    redStatus = LAMP_STATUS_OK;
                }

            /*
            greenStatus = LAMP_STATUS_CODE.get(greenLightStatus) == null ?
                    LAMP_STATUS_OK : LAMP_STATUS_CODE.get(greenLightStatus);
            yellowStatus = LAMP_STATUS_CODE.get(yellowLightStatus) == null ?
                    LAMP_STATUS_OK : LAMP_STATUS_CODE.get(yellowLightStatus);
            redStatus = LAMP_STATUS_CODE.get(redLightStatus) == null ?
                    LAMP_STATUS_OK : LAMP_STATUS_CODE.get(redLightStatus);
            */

                Values values = new Values(
                        intersectionId,
                        semaphoreId,
                        semaphoreLatitude,
                        semaphoreLongitude,
                        semaphoreTimestampUTC,
                        greenStatus,
                        yellowStatus,
                        redStatus
                );

                collector.emit(values);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                SEMAPHORE_ID,
                SEMAPHORE_LATITUDE,
                SEMAPHORE_LONGITUDE,
                SEMAPHORE_TIMESTAMP_UTC,
                GREEN_LIGHT_STATUS,
                YELLOW_LIGHT_STATUS,
                RED_LIGHT_STATUS
        ));
    }

}
