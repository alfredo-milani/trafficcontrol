package it.uniroma2.sdcc.trafficcontrol.abstractsBolts;

import it.uniroma2.sdcc.trafficcontrol.entity.timeWindow.EventsMangerTimeWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.timeWindow.IClientTimeWindow;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractWindowedBolt extends BaseRichBolt {

    protected static final long DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    protected static final long DEFAULT_WINDOW_SIZE_IN_SECONDS = 10;
    private static final AtomicLong LAST_TIME_MS = new AtomicLong();

    private OutputCollector collector;
    private final long emitFrequencyInMillis;
    private final EventsMangerTimeWindow eventsTimeWindow;

    public AbstractWindowedBolt() {
        this(DEFAULT_WINDOW_SIZE_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public AbstractWindowedBolt(long windowSizeInSeconds) {
        this(windowSizeInSeconds, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public AbstractWindowedBolt(long windowSizeInSeconds, long emitFrequencyInSeconds) {
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

        this.emitFrequencyInMillis = TimeUnit.SECONDS.toMillis(emitFrequencyInSeconds);
        this.eventsTimeWindow = new EventsMangerTimeWindow(TimeUnit.SECONDS.toMillis(windowSizeInSeconds));
    }

    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        try {
            if (TupleUtils.isTick(tuple)) {
                eventsTimeWindow.updateWindow(emitFrequencyInMillis);
                eventsTimeWindow.moveEventsFromCurrentToExpired();
                eventsTimeWindow.copyEventsFromNewToCurrent();

                onTick(collector, eventsTimeWindow.copy());

                eventsTimeWindow.clearNewEvents();
                eventsTimeWindow.clearExpiredEvents();
            } else {
                eventsTimeWindow.addNewEvent(getTimestampToUse(tuple), tuple);

                onTupleReceived(tuple);
            }
        } catch (BadTuple e) {
            System.out.println(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    private Long getTimestampToUse(Tuple tuple)
            throws BadTuple {
        Long lastTime, timestampToUse = System.currentTimeMillis();
        // Per calcolare un timestamp unico condiviso tra più threads
        do {
            lastTime = LAST_TIME_MS.get();
            if (lastTime >= timestampToUse) timestampToUse = lastTime + 1;
        } while (!LAST_TIME_MS.compareAndSet(lastTime, timestampToUse));

        Long timestampFromTuple = getTimestampFrom(tuple);

        if (timestampFromTuple != null) {
            if (timestampFromTuple < eventsTimeWindow.getLowerBoundWindow()) {
                throw new BadTuple(String.format(
                        "%s rejects tuple with timestamp <%d>",
                        this.getClass().getSimpleName(),
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

    protected abstract void onTick(OutputCollector collector, IClientTimeWindow<Tuple> eventsWindow)
            throws Exception;

    protected void onTupleReceived(Tuple tuple) {

    }

    @Override
    public final Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>() {{
            put(
                    Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
                    TimeUnit.MILLISECONDS.toSeconds(emitFrequencyInMillis)
            );
        }};
    }

}
