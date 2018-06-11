package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

public interface IRankable extends Comparable<IRankable> {

    Object getObject();

    int getMeanIntersectionSpeed();

    /**
     * Note: We do not defensively copy the object wrapped by the IRankable.  It is passed as is.
     *
     * @return a defensive copy
     */
    IRankable copy();

}
