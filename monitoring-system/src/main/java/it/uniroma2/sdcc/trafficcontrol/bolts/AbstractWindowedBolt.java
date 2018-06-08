package it.uniroma2.sdcc.trafficcontrol.bolts;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractWindowedBolt extends BaseRichBolt implements IWindow<Tuple> {

    private OutputCollector collector;

    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_WINDOW_SIZE_IN_SECONDS = 10;

    private final int windowSizeInSeconds;
    private final long windowSizeInMillis;
    private final int emitFrequencyInSeconds;
    private final long emitFrequencyInMillis;
    private long lowerBoundWindow;
    private long upperBoundWindow;
    private final Map<Long, Tuple> newEventList;
    private final Map<Long, Tuple> currentEventList;
    private final Map<Long, Tuple> expiredEventList;

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

        this.windowSizeInSeconds = windowSizeInSeconds;
        this.windowSizeInMillis = this.windowSizeInSeconds * 1000;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        this.emitFrequencyInMillis = this.emitFrequencyInSeconds * 1000;
        this.lowerBoundWindow = System.currentTimeMillis();
        this.upperBoundWindow = this.lowerBoundWindow + this.emitFrequencyInMillis;
        this.currentEventList = new HashMap<>();
        this.newEventList = new HashMap<>();
        this.expiredEventList = new HashMap<>();
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        if (TupleUtils.isTick(tuple)) {
            // Aggiungo gli eventi occorsi nello sliding interval nella lista degli eventi correnti
            currentEventList.putAll(newEventList);

            // Avanzamento head finestra temporale
            upperBoundWindow += emitFrequencyInMillis;

            if (upperBoundWindow - lowerBoundWindow > windowSizeInMillis) {
                // La finestra dal tempo 0 è >= windowSize
                // Avanzamento tail finestra temporale
                lowerBoundWindow += emitFrequencyInMillis;

                /*currentEventList.keySet().forEach(k -> {
                    if (k < lowerBoundWindow) {
                        expiredEventList.put(k, currentEventList.get(k));
                    }
                });
                currentEventList.keySet().removeIf(k -> k < lowerBoundWindow);

                Necessarie le ConcurrentHashMap per l'uso seguente
                currentEventList.keySet().forEach(k -> {
                    if (k < lowerBoundWindow) {
                        expiredEventList.put(k, currentEventList.get(k));
                        currentEventList.remove(k);
                    }
                });*/

                Iterator<Map.Entry<Long, Tuple>> iterator = currentEventList.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Long, Tuple> record = iterator.next();
                    if (record.getKey() < lowerBoundWindow) {
                        // Aggiunta dei valori eliminati in expiredEventList
                        expiredEventList.put(record.getKey(), record.getValue());
                        // Eliminazione eventi usciti dalla finestra temporale corrente
                        iterator.remove();
                    }
                }
            }

            onTick(collector);

            newEventList.clear();
            expiredEventList.clear();
        } else {
            newEventList.put(System.currentTimeMillis(), tuple);

            onTupleReceived(tuple);
        }
    }

    protected abstract void onTick(OutputCollector collector);

    protected abstract void onTupleReceived(Tuple tuple);

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }

    @Override
    public final ArrayList<Tuple> getNewEvents() {
        return new ArrayList<>(newEventList.values());
    }

    @Override
    public final ArrayList<Tuple> getCurrentEvents() {
        return new ArrayList<>(currentEventList.values());
    }

    @Override
    public final ArrayList<Tuple> getExpiredEvents() {
        return new ArrayList<>(expiredEventList.values());
    }

}
