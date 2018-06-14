package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.entity.ISensor;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import static it.uniroma2.sdcc.trafficcontrol.constants.MeanIntersectionTuple.MEAN_INTERSECTION_SPEED;
import static it.uniroma2.sdcc.trafficcontrol.constants.MeanIntersectionTuple.MEAN_INTERSECTION_SPEED_TIMESTAMP;
import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.INTERSECTION_ID;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;

public class MeanSpeedIntersectionRankable implements IRankable, ISensor {

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
                meanSpeedIntersection.getOldestSemaphoreTimestam()
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

    public Object getId() {
        return intersectionId;
    }

    public Integer getValue() {
        return meanIntersectionSpeed;
    }

    public Long getTimestamp() {
        return timestamp;
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MeanSpeedIntersectionRankable)) {
            return false;
        }

        MeanSpeedIntersectionRankable other = (MeanSpeedIntersectionRankable) o;
        /*return intersectionId.equals(other.intersectionId) &&
                meanIntersectionSpeed.equals(other.meanIntersectionSpeed) &&
                timestamp.equals(other.timestamp);*/
        return intersectionId.equals(other.getId()) &&
                meanIntersectionSpeed.equals(other.getValue()) &&
                timestamp.equals(other.getTimestamp());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                intersectionId,
                meanIntersectionSpeed,
                timestamp
        );
    }

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
