package it.uniroma2.sdcc.trafficcontrol.entity.timeWindow;

public interface IMangerTimeWindow<T> extends IClientTimeWindow<T> {

    void updateWindow(long interval);

    void addNewEvent(long timestamp, T t);

    void addCurrentEvent(long timestamp, T t);

    void addExpiredEvent(long timestamp, T t);

    void clearNewEvents();

    void clearCurrentEvents();

    void clearExpiredEvents();

    void copyEventsFromNewToCurrent();

    void moveEventsFromCurrentToExpired();

}
