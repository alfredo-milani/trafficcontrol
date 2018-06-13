package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.entity.ISensor;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import org.apache.storm.tuple.Tuple;

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

    public boolean hasSameProperties(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MeanSpeedIntersectionRankable)) {
            return false;
        }

        MeanSpeedIntersectionRankable other = (MeanSpeedIntersectionRankable) o;
        return intersectionId.equals(other.intersectionId) &&
                meanIntersectionSpeed.equals(other.meanIntersectionSpeed);
    }

    public Object getId() {
        return intersectionId;
    }

    public Integer getValue() {
        return meanIntersectionSpeed;
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
        return intersectionId.equals(other.intersectionId) &&
                meanIntersectionSpeed.equals(other.meanIntersectionSpeed) &&
                timestamp.equals(other.timestamp);
    }

    @Override
    public int hashCode() {
        int result = 17;
        int countHash = (int) (meanIntersectionSpeed ^ ((long) meanIntersectionSpeed >>> 32));
        result = 31 * result + countHash;
        result = (int) (31 * result + intersectionId.hashCode() + timestamp);
        return result;
    }

    public String toString() {
        return String.format(
                "Mean speed: %d\t- Intersection %d\t<timestamp - %d>\n",
                meanIntersectionSpeed,
                intersectionId,
                timestamp
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
