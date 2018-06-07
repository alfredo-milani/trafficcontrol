package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;

public class IntersectionRankable implements Rankable, Serializable {

    private final static ObjectMapper mapper = new ObjectMapper();

    private final Long intersectionId;
    private final int meanIntersectionSpeed;
    private final ImmutableList<Object> fields;

    public IntersectionRankable(Long intersectionId, int meanIntersectionSpeed, Object... otherFields) {
        if (intersectionId < 0) {
            throw new IllegalArgumentException("Intersection id must not be >= 0");
        }
        if (meanIntersectionSpeed < 0) {
            throw new IllegalArgumentException("The meanIntersectionSpeed must be >= 0");
        }

        this.intersectionId = intersectionId;
        this.meanIntersectionSpeed = meanIntersectionSpeed;
        fields = ImmutableList.copyOf(otherFields);
    }

    /**
     * Construct a new instance based on the provided {@link Tuple}.
     * <p/>
     * This method expects the object to be ranked in the first field (index 0) of the provided tuple, and the number of
     * occurrences of the object (its meanIntersectionSpeed) in the second field (index 1). Any further fields in the tuple will be
     * extracted and tracked, too. These fields can be accessed via {link RankableObjectWithFields#getFields()}.
     *
     * @param tuple
     * @return new instance based on the provided tuple
     */
    public static IntersectionRankable getIntersectionRankableFrom(Tuple tuple) throws IllegalArgumentException {
        MeanSpeedIntersection meanSpeedIntersectionManager =
                (MeanSpeedIntersection) tuple.getValueByField(INTERSECTION_MEAN_SPEED_OBJECT);

        Object[] k = {6, 9};
        return new IntersectionRankable(
                meanSpeedIntersectionManager.getIntersectionId(),
                meanSpeedIntersectionManager.getMeanIntersectionSpeed(),
                k
        );
    }

    public Object getObject() {
        return intersectionId;
    }

    public int getMeanIntersectionSpeed() {
        return meanIntersectionSpeed;
    }

    /**
     * @return an immutable list of any additional data fields of the object (may be empty but will never be null)
     */
    public List<Object> getFields() {
        return fields;
    }

    @Override
    public int compareTo(Rankable other) {
        long delta = this.getMeanIntersectionSpeed() - other.getMeanIntersectionSpeed();
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
        if (!(o instanceof IntersectionRankable)) {
            return false;
        }
        IntersectionRankable other = (IntersectionRankable) o;
        return intersectionId.equals(other.intersectionId) &&
                meanIntersectionSpeed == other.meanIntersectionSpeed;
    }

    @Override
    public int hashCode() {
        int result = 17;
        int countHash = (int) (meanIntersectionSpeed ^ (meanIntersectionSpeed >>> 32));
        result = 31 * result + countHash;
        result = 31 * result + intersectionId.hashCode();
        return result;
    }

    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("INTERSECTION RANKABLE -\t")
                .append(String.format("Mean intersection speed: %d\t", meanIntersectionSpeed))
                .append(String.format("Intersection id: %d", intersectionId));

        return buf.toString();
    }

    /**
     * Note: We do not defensively copy the wrapped object and any accompanying fields.  We do guarantee, however,
     * do return a defensive (shallow) copy of the List object that is wrapping any accompanying fields.
     *
     * @return
     */
    @Override
    public Rankable copy() {
        List<Object> shallowCopyOfFields = ImmutableList.copyOf(getFields());
        return new IntersectionRankable(
                (Long) getObject(),
                getMeanIntersectionSpeed(),
                shallowCopyOfFields
        );
    }

}
