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
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public abstract class AbstractWindowedBolt extends BaseRichBolt {

    protected static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    protected static final int DEFAULT_WINDOW_SIZE_IN_SECONDS = 10;
    private static final AtomicLong LAST_TIME_MS = new AtomicLong();

    private OutputCollector collector;
    private final int windowSizeInSeconds;
    private final long windowSizeInMillis;
    private final int emitFrequencyInSeconds;
    private final long emitFrequencyInMillis;
    private final EventsWindow eventsWindow;
    private boolean isWindowSlidingTotally;
    private long lowerBoundWindow;
    private long upperBoundWindow;

    private final class EventsWindow implements IWindow<Tuple>, Serializable {

        private final Map<Long, Tuple> newEventMap;
        private final Map<Long, Tuple> currentEventMap;
        private final Map<Long, Tuple> expiredEventMap;

        private EventsWindow() {
            this.newEventMap = new HashMap<>();
            this.currentEventMap = new HashMap<>();
            this.expiredEventMap = new HashMap<>();
        }

        @Override
        public final ArrayList<Tuple> getNewEvents() {
            return new ArrayList<>(newEventMap.values());
        }

        @Override
        public final ArrayList<Tuple> getCurrentEvents() {
            return new ArrayList<>(currentEventMap.values());
        }

        @Override
        public final ArrayList<Tuple> getExpiredEvents() {
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
                if (isWindowSlidingTotally) fillExpiredEventsAndRemoveFromCurrent();
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

    /**
     * Aggiorna i valori {@link AbstractWindowedBolt#upperBoundWindow} e {@link AbstractWindowedBolt#lowerBoundWindow}
     *
     * Assegna {@code isWindowSlidingTotally = true} sse è stato aggiornato il valore {@link AbstractWindowedBolt#lowerBoundWindow}
     * {@code isWindowSlidingTotally = false} altrimenti
     */
    private void updateWindow() {
        // Avanzamento head finestra temporale
        upperBoundWindow += emitFrequencyInMillis;

        long deltaHeadTail = upperBoundWindow - lowerBoundWindow;
        if (deltaHeadTail > windowSizeInMillis) {
            // La finestra dal tempo 0 è > windowSize
            // Avanzamento tail finestra temporale
            lowerBoundWindow += deltaHeadTail - windowSizeInMillis;
            isWindowSlidingTotally = true;
        }
    }

    private void fillNewEvents(Long timestampToUse, Tuple tuple) {
        eventsWindow.getNewEventsMap().put(timestampToUse, tuple);
    }

    private void fillCurrentEvents() {
        // Aggiungo gli eventi occorsi nello sliding interval nella lista degli eventi correnti
        eventsWindow.getCurrentEventsMap().putAll(eventsWindow.getNewEventsMap());
    }

    private void fillExpiredEventsAndRemoveFromCurrent() {
        /*eventsWindow.getCurrentEventsMap().keySet().forEach(k -> {
            if (k < lowerBoundWindow) {
                eventsWindow.getExpiredEventsMap().put(k, eventsWindow.getCurrentEventsMap().get(k));
            }
        });
        eventsWindow.getCurrentEventsMap().keySet().removeIf(k -> k < lowerBoundWindow);

        Necessarie le ConcurrentHashMap per l'uso seguente
        currentEventMap.keySet().forEach(k -> {
            if (k < lowerBoundWindow) {
                expiredEventMap.put(k, currentEventMap.get(k));
                currentEventMap.remove(k);
            }
        });*/

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
        Long lastTime, timestampToUse = System.currentTimeMillis();
        /*
        Per calcolare un timestamp unico condiviso tra più threads
        do {
            lastTime = LAST_TIME_MS.get();
            if (lastTime >= timestampToUse) timestampToUse = lastTime + 1;
        } while (!LAST_TIME_MS.compareAndSet(lastTime, timestampToUse));
        */

        Long timestampFromTuple = getTimestampFrom(tuple);
        if (timestampFromTuple != null) {
            // TODO vedere se mettere anche l'upper bound per il timestamp delle tuple (e vedere se le tuple vengono accettate)
            if (timestampFromTuple < lowerBoundWindow /* || timestampFromTuple > upperBoundWindow + emitFrequencyInMillis */) {
                throw new BadTuple(String.format(
                        "%s rejects tuple with timestamp <%d>",
                        getClassName(),
                        timestampFromTuple
                ));
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

    protected void onValidTupleReceived(Tuple tuple) {

    }

    @Override
    public final Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    protected long getLowerBoundWindow() {
        return lowerBoundWindow;
    }

    protected long getUpperBoundWindow() {
        return upperBoundWindow;
    }

    public boolean isWindowSlidingTotally() {
        return isWindowSlidingTotally;
    }

    public abstract String getClassName();

    public abstract Logger getLogger();

}
