package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

public interface Rankable extends Comparable<Rankable> {

    Object getObject();

    long getMeanIntersectionSpeed();

    /**
     * Note: We do not defensively copy the object wrapped by the Rankable.  It is passed as is.
     *
     * @return a defensive copy
     */
    Rankable copy();

}
