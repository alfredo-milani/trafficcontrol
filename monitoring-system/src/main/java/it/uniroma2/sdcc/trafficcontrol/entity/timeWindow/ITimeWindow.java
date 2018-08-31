package it.uniroma2.sdcc.trafficcontrol.entity.timeWindow;

import java.util.ArrayList;

public interface ITimeWindow<T> {

    long getLowerBoundWindow();

    long getUpperBoundWindow();

    void updateWindow(long interval);

    void addNewEvent(long timestamp, T t);

    void addCurrentEvent(long timestamp, T t);

    void addExpiredEvent(long timestamp, T t);

    ArrayList<T> getNewEvents();

    ArrayList<T> getCurrentEvents();

    ArrayList<T> getExpiredEvents();

    void clearNewEvents();

    void clearCurrentEvents();

    void clearExpiredEvents();

    void copyEventsFromNewToCurrent();

    void moveEventsFromCurrentToExpired();

}
