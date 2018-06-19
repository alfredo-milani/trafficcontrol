package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.logging.Logger;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_RANKINGS_OBJECT;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.PARTIAL_RANKINGS_OBJECT;

public class GlobalWindowedRankingsBolt extends AbstractWindowedBolt {

    private final static String CLASS_NAME = GlobalWindowedRankingsBolt.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    private final static int TOP_N_DEFAULT = 10;

    private final Rankings rankings;

    public GlobalWindowedRankingsBolt() {
        this(TOP_N_DEFAULT);
    }

    public GlobalWindowedRankingsBolt(int topN) {
        this(
                topN,
                AbstractWindowedBolt.DEFAULT_WINDOW_SIZE_IN_SECONDS,
                AbstractWindowedBolt.DEFAULT_EMIT_FREQUENCY_IN_SECONDS
        );
    }

    public GlobalWindowedRankingsBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        this(TOP_N_DEFAULT, windowSizeInSeconds, emitFrequencyInSeconds);
    }

    public GlobalWindowedRankingsBolt(int topN, int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);

        if (topN < 1) {
            throw new IllegalArgumentException(String.format("TopN must be >= 1 (you requested %d)", topN));
        }

        this.rankings = new Rankings(topN);
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        Rankings oldRankings = rankings.copy();

        if (isWindowSlidingTotally()) {
            rankings.getRankings().forEach(r -> {
                if (r.getTimestamp() < getLowerBoundWindow()) rankings.removeIfExists(r);
            });
        }
        eventsWindow.getNewEvents().forEach(
                t -> this.rankings.updateWith((Rankings) t.getValueByField(PARTIAL_RANKINGS_OBJECT))
        );

        // TODO a volte stampa la stessa classifca anche se non è cambiata (penso sia un errore di Java)
        if (!rankings.equals(oldRankings)) {
            collector.emit(new Values(rankings));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(GLOBAL_RANKINGS_OBJECT));
    }

    @Override
    public String getClassName() {
        return CLASS_NAME;
    }

    @Override
    public Logger getLogger() {
        return LOGGER;
    }

}
