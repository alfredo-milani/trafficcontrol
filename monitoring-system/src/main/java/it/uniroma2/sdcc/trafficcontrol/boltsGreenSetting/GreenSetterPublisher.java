package it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.entity.greenTiming.GreenTemporizationIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.SemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_EMIT_FREQUENCY;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.*;

public class GreenSetterPublisher extends AbstractKafkaPublisherBolt<String> {

    // portata di saturazione per larghezze di carreggiata inferiori a 5.5 metri
    // 1850 per carreggiate di 3.05 metri(ovvero le strade urbane)
    private int s = 1850;
    private int ip = 7;  //intevallo di cambio
    private int greenValueEven = 0;
    private int greenValueOdd = 0;
    private int cycleDuration = 200;
    private int L = 4; //tempo perso

    private final static ObjectMapper mapper = new ObjectMapper();

    public GreenSetterPublisher(AppConfig appConfig, String topic) {
        super(appConfig, topic);
    }

    @SuppressWarnings("Duplicates")
    @Override
    protected List<String> computeValueToPublish(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        ArrayList<String> strings = new ArrayList<>();
        GreenTemporizationIntersection greenTemporizationManager = (GreenTemporizationIntersection) tuple.getValueByField(GREEN_TEMPORIZATION_VALUE);

        List<SemaphoreSensor> evenSensors =  greenTemporizationManager.getSemaphoreSensorsEven();
        List<SemaphoreSensor> oddSensors =  greenTemporizationManager.getSemaphoreSensorsOdd();
        float q0,q1,q2,q3;


        // ObjectMapper mapper = new ObjectMapper();
        if (evenSensors.size() == 2){
            q0 = (float) evenSensors.get(0).getVehiclesNumber() / (float) SEMAPHORE_EMIT_FREQUENCY;
            q1 = (float) evenSensors.get(1).getVehiclesNumber() / (float) SEMAPHORE_EMIT_FREQUENCY;

            float maxQ = Math.max(q0,q1);

            int greenEffective = (int) (((maxQ / (float) s) * ((float) cycleDuration - (float) 2 * L)) / (q0 / (float) s + q1 / (float) s));

            greenValueEven = greenEffective + L - ip;

            // caso di un sensore relativo ad un semaforo senza veicoli in transito
            // il verde viene impostato ad un valore di default
            if (greenValueEven <= 0)
                greenValueEven = 100;

            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put(INTERSECTION_ID, greenTemporizationManager.getIntersectionId());
            objectNode.put(SEMAPHORE_SIDE, SEMAPHORE_SIDE_EVEN);
            objectNode.put(GREEN_TEMPORIZATION_VALUE, greenValueEven);

            strings.add(objectNode.toString());

            // strings.add(printArray(greenTemporizationManager.getIntersectionId(),"even", greenValueEven));
        }

        if (oddSensors.size() == 2){
            q2 = (float) oddSensors.get(0).getVehiclesNumber() / (float) SEMAPHORE_EMIT_FREQUENCY;
            q3 = (float) oddSensors.get(1).getVehiclesNumber() / (float) SEMAPHORE_EMIT_FREQUENCY;

            float maxQ = Math.max(q2,q3);

            int greenEffective = (int) (((maxQ / (float) s) * ((float) cycleDuration - (float) 2 * L)) / (q2 / (float) s + q3 / (float) s));

            greenValueOdd = greenEffective + L - ip;

            if (greenValueOdd <= 0)
                greenValueOdd = 100;

            ObjectNode objectNode = mapper.createObjectNode();
            objectNode.put(INTERSECTION_ID, greenTemporizationManager.getIntersectionId());
            objectNode.put(SEMAPHORE_SIDE, SEMAPHORE_SIDE_ODD);
            objectNode.put(GREEN_TEMPORIZATION_VALUE, greenValueOdd);
            strings.add(objectNode.toString());

            // strings.add(printArray(greenTemporizationManager.getIntersectionId(),"odd", greenValueOdd));

        }

        return strings;
    }

    private String printArray(Long intersectionId, String sideType, int value){
        StringBuilder text= new StringBuilder();
        text.append("Intersection ID: ").append(intersectionId).append("\t\t")
                .append("Green value: ").append(value).append("  \t")
                .append("Side type: ").append(sideType);
        return String.valueOf(text);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
