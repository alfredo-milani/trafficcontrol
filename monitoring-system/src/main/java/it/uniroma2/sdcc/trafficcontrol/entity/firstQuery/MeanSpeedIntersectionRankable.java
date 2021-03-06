package it.uniroma2.sdcc.trafficcontrol.entity.firstQuery;

import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.IRankable;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.ITupleObject;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Date;

import static it.uniroma2.sdcc.trafficcontrol.constants.MeanIntersectionTuple.MEAN_INTERSECTION_SPEED;
import static it.uniroma2.sdcc.trafficcontrol.constants.MeanIntersectionTuple.MEAN_INTERSECTION_SPEED_TIMESTAMP;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;

@Getter
@Setter
@EqualsAndHashCode
public class MeanSpeedIntersectionRankable implements IRankable, ITupleObject {

    private final Long intersectionId;
    private final Integer meanIntersectionSpeed;
    private final Long timestamp;

    public MeanSpeedIntersectionRankable(Long intersectionId, Integer meanIntersectionSpeed, Long timestamp) {
        if (intersectionId < 0) {
            throw new IllegalArgumentException("Intersection id must not be >= 0");
        } else if (meanIntersectionSpeed < 0) {
            throw new IllegalArgumentException("The meanIntersectionSpeed must be >= 0");
        } else if (timestamp < 0) {
            throw new IllegalArgumentException("The timestamp must be > 0");
        }

        this.intersectionId = intersectionId;
        this.meanIntersectionSpeed = meanIntersectionSpeed;
        this.timestamp = timestamp;
    }

    public static MeanSpeedIntersectionRankable getInstanceFrom(Tuple tuple) {
        MeanSpeedIntersection meanSpeedIntersection = (MeanSpeedIntersection) tuple.getValueByField(INTERSECTION_MEAN_SPEED_OBJECT);

        return new MeanSpeedIntersectionRankable(
                meanSpeedIntersection.getIntersectionId(),
                meanSpeedIntersection.getMeanIntersectionSpeed(),
                meanSpeedIntersection.getOldestSemaphoreTimestamp()
        );
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();
        objectNode.put(INTERSECTION_ID, intersectionId);
        objectNode.put(MEAN_INTERSECTION_SPEED, meanIntersectionSpeed);
        objectNode.put(MEAN_INTERSECTION_SPEED_TIMESTAMP, timestamp);

        return objectNode.toString();
    }

    @Override
    public Object getId() {
        return getIntersectionId();
    }

    @Override
    public Integer getValue() {
        return getMeanIntersectionSpeed();
    }

    @Override
    public int compareTo(IRankable other) {
        long delta = this.getValue() - other.getValue();
        if (delta > 0) {
            return 1;
        } else if (delta < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "Mean speed: %d\t- Intersection %d\t<timestamp - %s>\n",
                meanIntersectionSpeed,
                intersectionId,
                new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(timestamp))
        );
    }

    @Override
    public IRankable copy() {
        return new MeanSpeedIntersectionRankable(
                (Long) getId(),
                getValue(),
                timestamp
        );
    }

}
