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
    @EqualsAndHashCode.Include private Double mainCoordinate;
    @EqualsAndHashCode.Include private Double initialCoordinate;
    @EqualsAndHashCode.Include private Double finalCoordinate;
    @EqualsAndHashCode.Include private SequenceType sequenceType;
    private List<RichMobileSensor> sensorsInSequence;
    private Double congestionGrade;

    /**
     * Rappresenta la disposizione spaziale della sequenza di semafori.
     *
     * Una sequenza di semafori di tipo {@link SequenceType#LONGITUDINAL} indica che
     * l'intera sequenza giace sulla stessa longitudine definita da {@link SemaphoresSequence#mainCoordinate}
     * e si estende dalla coordinata definita da {@link SemaphoresSequence#initialCoordinate} e {@link SemaphoresSequence#finalCoordinate}
     *
     * Allo stesso modo, una sequenza di semafori di tipo {@link SequenceType#LATITUDINAL} indica che
     * l'intera sequenza giace sulla stessa latitudine definita da {@link SemaphoresSequence#mainCoordinate}
     * e si estende dalla coordinata definita da {@link SemaphoresSequence#initialCoordinate} e {@link SemaphoresSequence#finalCoordinate}
     */
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

        FIRST((short) 0, (short) 10, 0.1650),
        SECOND((short) 11, (short) 20, 0.1575),
        THIRD((short) 21, (short) 30, 0.1550),
        FOURTH((short) 31, (short) 40, 0.1525),
        FIFTH((short) 41, (short) 50, 0.1050),
        SIXTH((short) 51, (short) 60, 0.1030),
        SEVENTH((short) 61, (short) 70, 0.1020),
        EIGHTH((short) 71, (short) 80, 0.03),
        NINTH((short) 81, (short) 90, 0.02),
        TENTH((short) 91, (short) 100, 0.01),
        ELEVENTH((short) 101, Short.MAX_VALUE, 0.0);

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

    public SemaphoresSequence(List<Long> semaphoresSequence, Double mainCoordinate,
                              Double initialCoordinate, Double finalCoordinate, SequenceType sequenceType,
                              List<RichMobileSensor> sensorsInSequence, Double congestionGrade) {
        this.semaphoresSequence = semaphoresSequence;
        this.mainCoordinate = mainCoordinate;
        this.initialCoordinate = initialCoordinate;
        this.finalCoordinate = finalCoordinate;
        this.sequenceType = sequenceType;
        this.sensorsInSequence = sensorsInSequence;
        this.congestionGrade = congestionGrade;
    }

    public Double computeCongestionGrade() {
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
                semaphoresSequence, mainCoordinate, initialCoordinate,
                finalCoordinate, sequenceType, null,
                congestionGrade
        );
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(CONGESTED_SEQUENCE_PRINT_TIMESTAMP, System.currentTimeMillis());
        ObjectNode sequenceNode = objectNode.putObject(CONGESTED_SEQUENCE_INSTANCE);
        ArrayNode semaphoreSequenceArrayNode = sequenceNode.putArray(SEMAPHORES_SEQUENCE);
        semaphoresSequence.forEach(semaphoreSequenceArrayNode::add);
        sequenceNode.put(SEQUENCE_SEMAPHORE_MAIN_COORDINATE, mainCoordinate);
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
        buffer.append(String.format("|  main coordinate: %.6f\n", mainCoordinate));
        buffer.append(String.format("|  initial coordinate: %.6f\n", initialCoordinate));
        buffer.append(String.format("|  final coordinate: %.6f\n", finalCoordinate));
        buffer.append(String.format("|  sequence type: %s\n", sequenceType.toString()));
        buffer.append(String.format("|_ congestion grade: %.3f\n", congestionGrade));

        return buffer.toString();
    }

}
