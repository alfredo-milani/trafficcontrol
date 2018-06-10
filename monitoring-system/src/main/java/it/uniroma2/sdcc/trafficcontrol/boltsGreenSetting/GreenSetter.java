package it.uniroma2.sdcc.trafficcontrol.boltsGreenSetting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.GreenTemporization;
import it.uniroma2.sdcc.trafficcontrol.entity.SemaphoreSensor;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.EVEN_SEMAPHORES;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.ODD_SEMAPHORES;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_EMIT_FREQUENCY;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GREEN_TEMPORIZATION_VALUE;

public class GreenSetter extends AbstractKafkaPublisherBolt<String> {

    private int s=1850;
    private int ip = 5;
    private int greenValueEven= 0;
    private int greenValueOdd= 0;
    private int cycleDuration = 200;
    private int L = 4;

    public GreenSetter(String topic) {
        super(topic);
    }

    @Override
    protected ArrayList<String> computeStringToPublish(Tuple tuple) throws ClassCastException, IllegalArgumentException {
        ArrayList<String> strings = new ArrayList<>();
        GreenTemporization greenTemporizationManager = (GreenTemporization) tuple.getValueByField(GREEN_TEMPORIZATION_VALUE);

        List<SemaphoreSensor> evenSensors =  greenTemporizationManager.getSemaphoreSensorsEven();
        List<SemaphoreSensor> oddSensors =  greenTemporizationManager.getSemaphoreSensorsOdd();
        int q0,q1,q2,q3;

        ObjectMapper mapper = new ObjectMapper();
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

            strings.add(objectNode.toString());
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

            strings.add(objectNode.toString());
        }

        return strings;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
