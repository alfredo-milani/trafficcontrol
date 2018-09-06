package it.uniroma2.sdcc.trafficcontrol.entity.timeWindow;

import lombok.Getter;
import org.apache.storm.tuple.Tuple;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventsMangerTimeWindow implements IMangerTimeWindow<Tuple>, Serializable {

    @Getter private final long windowSize;
    @Getter private long lowerBoundWindow;
    @Getter private long upperBoundWindow;
    private final Map<Long, Tuple> newEventsMap;
    private final Map<Long, Tuple> currentEventsMap;
    private final Map<Long, Tuple> expiredEventsMap;

    public EventsMangerTimeWindow(long windowSize) {
        this.windowSize = windowSize;
        lowerBoundWindow = upperBoundWindow = System.currentTimeMillis();
        this.newEventsMap = new HashMap<>();
        this.currentEventsMap = new HashMap<>();
        this.expiredEventsMap = new HashMap<>();
    }

    /**
     * Aggiorna i valori {@link EventsMangerTimeWindow#upperBoundWindow} e {@link EventsMangerTimeWindow#lowerBoundWindow}
     */
    @Override
    public void updateWindow(long interval) {
        // Avanzamento head finestra temporale
        upperBoundWindow += interval;

        long deltaHeadTail = upperBoundWindow - lowerBoundWindow;
        if (deltaHeadTail > windowSize) {
            // La finestra è diventata maggiore di windowSize
            // quindi aggiorniamo lowerBoundWindow della finestra temporale
            lowerBoundWindow += deltaHeadTail - windowSize;
        }
    }

    @Override
    public void addNewEvent(long timestamp, @NotNull Tuple tuple) {
        newEventsMap.put(timestamp, tuple);
    }

    @Override
    public void addCurrentEvent(long timestamp, @NotNull Tuple tuple) {
        currentEventsMap.put(timestamp, tuple);
    }

    @Override
    public void addExpiredEvent(long timestamp, @NotNull Tuple tuple) {
        expiredEventsMap.put(timestamp, tuple);
    }

    @Override
    public final @NotNull ArrayList<Tuple> getNewEvents() {
        return new ArrayList<>(newEventsMap.values());
    }

    @Override
    public final @NotNull ArrayList<Tuple> getCurrentEvents() {
        return new ArrayList<>(currentEventsMap.values());
    }

    @Override
    public final @NotNull ArrayList<Tuple> getExpiredEvents() {
        return new ArrayList<>(expiredEventsMap.values());
    }

    @Override
    public void clearNewEvents() {
        newEventsMap.clear();
    }

    @Override
    public void clearCurrentEvents() {
        currentEventsMap.clear();
    }

    @Override
    public void clearExpiredEvents() {
        expiredEventsMap.clear();
    }

    @Override
    public void copyEventsFromNewToCurrent() {
        // Aggiungo gli eventi occorsi nello sliding interval nella lista degli eventi correnti
        currentEventsMap.putAll(newEventsMap);
    }

    @Override
    public void moveEventsFromCurrentToExpired() {
        /*
        O(n^2) -> task troppo oneroso
        currentEventsMap.keySet().forEach(k -> {
            if (k < lowerBoundWindow) {
                expiredEventsMap.put(k, currentEventsMap.get(k));
            }
        });
        currentEventsMap.keySet().removeIf(k -> k < lowerBoundWindow);

        Necessarie le ConcurrentHashMap (task troppo oneroso) per l'uso seguente:
        currentEventsMap.keySet().forEach(k -> {
            if (k < lowerBoundWindow) {
                expiredEventsMap.put(k, currentEventsMap.get(k));
                currentEventsMap.remove(k);
            }
        });
        */

        Iterator<Map.Entry<Long, Tuple>> iterator = currentEventsMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Tuple> record = iterator.next();
            if (record.getKey() < lowerBoundWindow) {
                // Aggiunta dei valori eliminati in expiredEventsMap
                expiredEventsMap.put(record.getKey(), record.getValue());
                // Eliminazione eventi usciti dalla finestra temporale corrente
                iterator.remove();
            }
        }
    }

    // Copia difensiva
    @Override
    public @NotNull EventsMangerTimeWindow copy() {
        EventsMangerTimeWindow emtw = new EventsMangerTimeWindow(windowSize);
        emtw.lowerBoundWindow = this.lowerBoundWindow;
        emtw.upperBoundWindow = this.upperBoundWindow;
        emtw.newEventsMap.putAll(this.newEventsMap);
        emtw.currentEventsMap.putAll(this.currentEventsMap);
        emtw.expiredEventsMap.putAll(this.expiredEventsMap);
        return emtw;
    }

}