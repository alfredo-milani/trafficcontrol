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
        if (sensorsInSequence.size() != 0) {

            // TODO

            return congestionGrade = 0.34;
        } else {
            return congestionGrade = 0.0;
        }
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
        buffer.append(String.format("| semaphore sequence: %s\n", semaphoresSequence.toArray()));
        buffer.append(String.format("| initial coordinate: %.6f\n", initialCoordinate));
        buffer.append(String.format("| initial coordinate: %.6f\n", finalCoordinate));
        buffer.append(String.format("| sequence type: %s\n", sequenceType.toString()));
        buffer.append(String.format("| congestion grade: %.3f\n", congestionGrade));
        buffer.append("\n");

        return buffer.toString();
    }

}
