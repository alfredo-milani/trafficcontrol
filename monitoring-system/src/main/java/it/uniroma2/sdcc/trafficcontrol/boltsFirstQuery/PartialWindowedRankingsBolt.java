package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.IntersectionRankable;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankable;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.PARTIAL_RANKINGS_OBJECT;

public class PartialWindowedRankingsBolt extends AbstractWindowedBolt {

    private final static int TOP_N_DEFAULT = 10;

    private final Rankings rankings;

    public PartialWindowedRankingsBolt() {
        this(TOP_N_DEFAULT);
    }

    public PartialWindowedRankingsBolt(int topN) {
        this(
                topN,
                AbstractWindowedBolt.DEFAULT_WINDOW_SIZE_IN_SECONDS,
                AbstractWindowedBolt.DEFAULT_EMIT_FREQUENCY_IN_SECONDS
        );
    }

    public PartialWindowedRankingsBolt(int windowSizeInSeconds, int emitFrequencyInSeconds) {
        this(TOP_N_DEFAULT, windowSizeInSeconds, emitFrequencyInSeconds);
    }

    public PartialWindowedRankingsBolt(int topN, int windowSizeInSeconds, int emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);

        if (topN < 1) {
            throw new IllegalArgumentException(String.format("TopN must be >= 1 (you requested %d)", topN));
        }

        this.rankings = new Rankings(topN);
    }

    @Override
    protected void onTick(OutputCollector collector, IWindow<Tuple> eventsWindow) {
        Rankings oldRankings = rankings.copy(false);

        eventsWindow.getExpiredEventsWindow().forEach(t -> {
            Rankable rankable = IntersectionRankable.getIntersectionRankableFrom(t);
            rankings.removeIfExists(rankable);
        });
        eventsWindow.getNewEventsWindow().forEach(t -> {
            Rankable rankable = IntersectionRankable.getIntersectionRankableFrom(t);
            rankings.updateWith(rankable);
        });

        if (!oldRankings.equals(rankings) && rankings.size() != 0) {
            collector.emit(new Values(rankings));
            // System.out.println(rankings.toString());
        }
    }

    @Override
    protected void onTupleReceived(Tuple tuple) {

    }

    @Override
    protected Long getTimestampFrom(Tuple tuple) {
        // TODO modificare classe IntersectionRankable
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PARTIAL_RANKINGS_OBJECT));
    }

}
