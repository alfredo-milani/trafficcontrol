package it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.entity.GreenTemporizationManager;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.SERIALIZER_VALUE;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_EMIT_FREQUENCY;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.VEHICLES;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GREEN_TEMPORIZATION_VALUE;

public class GreenSetter extends BaseRichBolt {

    private OutputCollector collector;
    private KafkaProducer<String, String> producer;
    private ObjectMapper mapper;


    private int s=1850;
    private int ip = 5;
    private int greenValueEven= 0;
    private int greenValueOdd= 0;
    private int cycleDuration = 200;
    private int L = 4;



    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;
        this.mapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
        props.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        props.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public void execute(Tuple input) {


        GreenTemporizationManager greenTemporizationManager = (GreenTemporizationManager) input.getValueByField(GREEN_TEMPORIZATION_VALUE);

        List<SemaphoreSensor> evenSensors =  greenTemporizationManager.getSemaphoreSensorsEven();
        List<SemaphoreSensor> oddSensors =  greenTemporizationManager.getSemaphoreSensorsOdd();
        int q0,q1,q2,q3;

        if(evenSensors.size()==2){
            q0 = evenSensors.get(0).getVehiclesNumber()/SEMAPHORE_EMIT_FREQUENCY;
            q1 = evenSensors.get(1).getVehiclesNumber()/SEMAPHORE_EMIT_FREQUENCY;

            int maxQ = Math.max(q0,q1);

            int greenEffective = (int) ((((float)maxQ/(float)s)*( (float)cycleDuration- (float)2*L))/((float) q0/ (float)s + (float)q1/ (float)s));

            greenValueEven = greenEffective + L - ip;

            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put(INTERSECTION_ID, greenTemporizationManager.getIntersectionId());
            objectNode.put(EVEN_SEMAPHORES,"even");
            objectNode.put(GREEN_TEMPORIZATION_VALUE, greenValueEven);

            producer.send(new ProducerRecord<>(GREEN_TEMPORIZATION, objectNode.toString()));



        }
        if(oddSensors.size()==2){
            q2 = oddSensors.get(0).getVehiclesNumber()/SEMAPHORE_EMIT_FREQUENCY;
            q3 = oddSensors.get(1).getVehiclesNumber()/SEMAPHORE_EMIT_FREQUENCY;

            int maxQ = Math.max(q2,q3);

            int greenEffective = (int) ((((float)maxQ/(float)s)*( (float)cycleDuration- (float)2*L))/((float) q2/ (float)s + (float)q3/ (float)s));

            greenValueOdd = greenEffective + L - ip;

            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put(INTERSECTION_ID, greenTemporizationManager.getIntersectionId());
            objectNode.put(ODD_SEMAPHORES,"odd");
            objectNode.put(GREEN_TEMPORIZATION_VALUE, greenValueOdd);

            producer.send(new ProducerRecord<>(GREEN_TEMPORIZATION, objectNode.toString()));


        }




    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
