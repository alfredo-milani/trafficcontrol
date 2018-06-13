package it.uniroma2.sdcc.trafficcontrol.bolts;

import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractWindowedBolt extends BaseRichBolt {

    private OutputCollector collector;

    protected static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    protected static final int DEFAULT_WINDOW_SIZE_IN_SECONDS = 10;

    private final int windowSizeInSeconds;
    private final long windowSizeInMillis;
    private final int emitFrequencyInSeconds;
    private final long emitFrequencyInMillis;
    private long lowerBoundWindow;
    private long upperBoundWindow;
    private final EventsWindow eventsWindow;

    private class EventsWindow implements IWindow<Tuple>, Serializable {

        private final Map<Long, Tuple> newEventMap;
        private final Map<Long, Tuple> currentEventMap;
        private final Map<Long, Tuple> expiredEventMap;

        private EventsWindow() {
            this.newEventMap = new HashMap<>();
            this.currentEventMap = new HashMap<>();
            this.expiredEventMap = new HashMap<>();
        }

        @Override
        public ArrayList<Tuple> getNewEventsWindow() {
            return new ArrayList<>(newEventMap.values());
        }

        @Override
        public ArrayList<Tuple> getCurrentEventsWindow() {
            return new ArrayList<>(currentEventMap.values());
        }

        @Override
        public ArrayList<Tuple> getExpiredEventsWindow() {
            return new ArrayList<>(expiredEventMap.values());
        }

        private Map<Long, Tuple> getNewEventsMap() {
            return newEventMap;
        }

        private Map<Long, Tuple> getCurrentEventsMap() {
            return currentEventMap;
        }

        private Map<Long, Tuple> getExpiredEventsMap() {
            return expiredEventMap;
        }

    }

    public AbstractWindowedBolt() {
        this(DEFAULT_WINDOW_SIZE_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public AbstractWindowedBolt(int windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public AbstractWindowedBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        if (windowSizeInSeconds < 1) {
            throw new IllegalArgumentException(
                    String.format("windowSizeInSeconds must be >= 1 (you requested %d)", windowSizeInSeconds)
            );
        }
        if (emitFrequencyInSeconds < 1) {
            throw new IllegalArgumentException(
                    String.format("The emit frequency must be >= 1 seconds (you requested %d seconds)", emitFrequencyInSeconds));
        }
        if (emitFrequencyInSeconds > windowSizeInSeconds) {
            throw new IllegalArgumentException(String.format(
                    "The emit frequency must be <= window size (you selected: frequency to %d and window to %d",
                    emitFrequencyInSeconds,
                    windowSizeInSeconds
            ));
        }

        this.windowSizeInSeconds = windowSizeInSeconds;
        this.windowSizeInMillis = this.windowSizeInSeconds * 1000;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        this.emitFrequencyInMillis = this.emitFrequencyInSeconds * 1000;
        this.eventsWindow = new EventsWindow();
        this.lowerBoundWindow = this.upperBoundWindow = System.currentTimeMillis();
    }

    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        try {
            if (TupleUtils.isTick(tuple)) {
                updateWindow();

                fillExpiredEventsAndRemoveFromCurrentEvents();
                fillCurrentEvents();

                onTick(collector, eventsWindow);

                clearNewAndExpiredEvents();
            } else {
                fillNewEvents(getTimestampToUse(tuple), tuple);

                onValidTupleReceived(tuple);
            }
        } catch (BadTuple e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    private void updateWindow() {
        // Avanzamento head finestra temporale
        upperBoundWindow += emitFrequencyInMillis;

        long deltaHeadTail = upperBoundWindow - lowerBoundWindow;
        if (deltaHeadTail > windowSizeInMillis) {
            // La finestra dal tempo 0 è > windowSize
            // Avanzamento tail finestra temporale
            lowerBoundWindow += deltaHeadTail - windowSizeInMillis;
        }
    }

    private void fillNewEvents(Long timestampToUse, Tuple tuple) {
        eventsWindow.getNewEventsMap().put(timestampToUse, tuple);
    }

    private void fillCurrentEvents() {
        // Aggiungo gli eventi occorsi nello sliding interval nella lista degli eventi correnti
        eventsWindow.getCurrentEventsMap().putAll(eventsWindow.getNewEventsMap());
    }

    private void fillExpiredEventsAndRemoveFromCurrentEvents() {
        // TODO vedere se togliere l'if
        /*if (upperBoundWindow - lowerBoundWindow > windowSizeInMillis) {
            Iterator<Map.Entry<Long, Tuple>> iterator = eventsWindow.getCurrentEventsMap().entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Long, Tuple> record = iterator.next();
                if (record.getKey() < lowerBoundWindow) {
                    // Aggiunta dei valori eliminati in expiredEventMap
                    eventsWindow.getExpiredEventsMap().put(record.getKey(), record.getValue());
                    // Eliminazione eventi usciti dalla finestra temporale corrente
                    iterator.remove();
                }
            }
        }*/

        Iterator<Map.Entry<Long, Tuple>> iterator = eventsWindow.getCurrentEventsMap().entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, Tuple> record = iterator.next();
            if (record.getKey() < lowerBoundWindow) {
                // Aggiunta dei valori eliminati in expiredEventMap
                eventsWindow.getExpiredEventsMap().put(record.getKey(), record.getValue());
                // Eliminazione eventi usciti dalla finestra temporale corrente
                iterator.remove();
            }
        }
    }

    private Long getTimestampToUse(Tuple tuple) throws BadTuple {
        Long timestampToUse = System.currentTimeMillis();
        Long timestampFromTuple = getTimestampFrom(tuple);
        if (timestampFromTuple != null) {
            if (timestampFromTuple < lowerBoundWindow /* TODO non chiaro... || timestampFromTuple > upperBoundWindow + emitFrequencyInMillis */) {
                throw new BadTuple(String.format("Timestamp not allowed: %d", timestampFromTuple));
            }
            timestampToUse = timestampFromTuple;
        }

        return timestampToUse;
    }

    /**
     * Metodo opzionale che permette di utilizzare un eventuale timestamp contenuto all'interno della tupla.
     * Se invece si vuole utilizzare il timestamp di arrivo della tupla nel bolt {@link AbstractWindowedBolt} per
     * determinare se una tupla è nuova o scaduta ritornare il valore null (comportamento di default)
     *
     * @param tuple Tupla dalla quale estrarre il timestamp
     * @return Timestamp
     */
    protected Long getTimestampFrom(Tuple tuple) {
        return null;
    }

    private void clearNewAndExpiredEvents() {
        eventsWindow.getNewEventsMap().clear();
        eventsWindow.getExpiredEventsMap().clear();
    }

    protected abstract void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow);

    protected abstract void onValidTupleReceived(Tuple tuple);

    @Override
    public final Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

}
