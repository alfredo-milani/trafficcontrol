package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.ITupleObject;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichMobileSensor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.*;

import static it.uniroma2.sdcc.trafficcontrol.constants.CongestedSequenceJsonFileds.*;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class SemaphoresSequence implements ITupleObject {

    @EqualsAndHashCode.Include private List<Long> semaphoresSequence;
    @EqualsAndHashCode.Include private Double initialCoordinate;
    @EqualsAndHashCode.Include private Double finalCoordinate;
    @EqualsAndHashCode.Include private SequenceType sequenceType;
    private List<RichMobileSensor> sensorsInSequence;
    private Double congestionGrade;

    public enum SequenceType {

        LATITUDINAL,
        LONGITUDINAL;

        private static Map<String, SequenceType> namesMap = new HashMap<String, SequenceType>() {{
            put(LATITUDINAL.toString(), LATITUDINAL);
            put(LONGITUDINAL.toString(), LONGITUDINAL);
        }};

        @JsonCreator
        public static SequenceType forValue(String value) {
            return namesMap.get(StringUtils.upperCase(value));
        }

    }

    public enum CongestionCategory {

        FIRST((short) 0, (short) 20, 0.42),
        SECOND((short) 21, (short) 40, 0.28),
        THIRD((short) 41, (short) 60, 0.20),
        FOURTH((short) 61, (short) 80, 0.075),
        FIFTH((short) 81, (short) 100, 0.025),
        SIXTH((short) 101, Short.MAX_VALUE, 0.0);

        private final Short v1;
        private final Short v2;
        private final Double value;

        CongestionCategory(Short v1, Short v2, Double value) {
            this.v1 = v1;
            this.v2 = v2;
            this.value = value;
        }

        public static CongestionCategory getCategory(Short vel) {
            for (CongestionCategory category : CongestionCategory.values()) {
                if (vel >= category.v1 && vel <= category.v2) {
                    return category;
                }
            }
            return null;
        }

    }

    public SemaphoresSequence() {
        sensorsInSequence = new ArrayList<>();
        congestionGrade = 0.0;
    }

    public SemaphoresSequence(List<Long> semaphoresSequence, Double initialCoordinate,
                              Double finalCoordinate, SequenceType sequenceType,
                              List<RichMobileSensor> sensorsInSequence, Double congestionGrade) {
        this.semaphoresSequence = semaphoresSequence;
        this.initialCoordinate = initialCoordinate;
        this.finalCoordinate = finalCoordinate;
        this.sequenceType = sequenceType;
        this.sensorsInSequence = sensorsInSequence;
        this.congestionGrade = congestionGrade;
    }

    public Double computeCongestionGrade() {

        // TODO CONTROLLARE CORRETTEZZA

        if (sensorsInSequence.size() != 0) {
            Map<CongestionCategory, Integer> categoryVehiclesMap = new HashMap<>(CongestionCategory.values().length);
            for (CongestionCategory category : CongestionCategory.values()) categoryVehiclesMap.put(category, 0);
            sensorsInSequence.forEach(s -> {
                CongestionCategory category = CongestionCategory.getCategory(s.getMobileSpeed());
                categoryVehiclesMap.put(category, categoryVehiclesMap.get(category) + 1);
            });

            Double nom = 0.0, den = 0.0;
            for (Map.Entry<CongestionCategory, Integer> entry : categoryVehiclesMap.entrySet()) {
                nom += entry.getKey().value * entry.getValue();
            }
            for (CongestionCategory category : CongestionCategory.values()) {
                den += category.value;
            }

            return congestionGrade = nom / den;
        } else {
            return congestionGrade = 0.0;
        }
    }

    public void addSensorInSequence(RichMobileSensor richMobileSensor) {
        sensorsInSequence.removeIf(s -> s.getMobileId().equals(richMobileSensor.getMobileId()));
        sensorsInSequence.add(richMobileSensor);
    }

    public void removeSensorInSequence(RichMobileSensor richMobileSensor) {
        sensorsInSequence.removeIf(s -> s.getMobileId().equals(richMobileSensor.getMobileId()));
    }

    public SemaphoresSequence createCopyToSend() {
        // Per ridurre overhead nella trasmissione non
        // trasmettiamo la lista dei sensori mobili
        return new SemaphoresSequence(
                semaphoresSequence, initialCoordinate, finalCoordinate,
                sequenceType, null, congestionGrade
        );
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(CONGESTED_SEQUENCE_PRINT_TIMESTAMP, System.currentTimeMillis());
        ObjectNode sequenceNode = objectNode.putObject(CONGESTED_SEQUENCE_INSTANCE);
        ArrayNode semaphoreSequenceArrayNode = sequenceNode.putArray(SEMAPHORES_SEQUENCE);
        semaphoresSequence.forEach(semaphoreSequenceArrayNode::add);
        sequenceNode.put(SEQUENCE_SEMAPHORE_INITIAL_COORDINATE, initialCoordinate);
        sequenceNode.put(SEQUENCE_SEMAPHORE_FINAL_COORDINATE, finalCoordinate);
        sequenceNode.put(SEQUENCE_TYPE, sequenceType.toString());
        sequenceNode.put(CONGESTION_GRADE, congestionGrade);

        return objectNode.toString();
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format(
                "Semaphore sequence <print timestamp - %s>\n",
                new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(System.currentTimeMillis()))
        ));
        StringBuilder semaphoreSequenceString = new StringBuilder();
        for (int i = 0; i < semaphoresSequence.toArray().length; ++i) {
            semaphoreSequenceString.append(semaphoresSequence.get(i));
            if (i != semaphoresSequence.toArray().length - 1) {
                semaphoreSequenceString.append(", ");
            }
        }
        buffer.append(String.format("|  semaphore sequence: %s\n", semaphoreSequenceString));
        buffer.append(String.format("|  initial coordinate: %.6f\n", initialCoordinate));
        buffer.append(String.format("|  initial coordinate: %.6f\n", finalCoordinate));
        buffer.append(String.format("|  sequence type: %s\n", sequenceType.toString()));
        buffer.append(String.format("|_ congestion grade: %.3f\n", congestionGrade));

        return buffer.toString();
    }

}
