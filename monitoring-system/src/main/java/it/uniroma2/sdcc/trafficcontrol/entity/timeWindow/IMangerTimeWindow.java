package it.uniroma2.sdcc.trafficcontrol.entity.timeWindow;

import javax.validation.constraints.NotNull;

public interface IMangerTimeWindow<T> extends IClientTimeWindow<T> {

    void updateWindow(long interval);

    void addNewEvent(long timestamp, @NotNull T t);

    void addCurrentEvent(long timestamp, @NotNull T t);

    void addExpiredEvent(long timestamp, @NotNull T t);

    void clearNewEvents();

    void clearCurrentEvents();

    void clearExpiredEvents();

    void copyEventsFromNewToCurrent();

    void moveEventsFromCurrentToExpired();

    @NotNull IMangerTimeWindow<T> copy();

}
