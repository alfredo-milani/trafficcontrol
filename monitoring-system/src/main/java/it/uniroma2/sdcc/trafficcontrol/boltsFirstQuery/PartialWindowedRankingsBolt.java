package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.firstQuery.MeanSpeedIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.firstQuery.MeanSpeedIntersectionRankable;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import it.uniroma2.sdcc.trafficcontrol.entity.timeWindow.IClientTimeWindow;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.INTERSECTION_MEAN_SPEED_OBJECT;
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

    public PartialWindowedRankingsBolt(long windowSizeInSeconds, long emitFrequencyInSeconds) {
        this(TOP_N_DEFAULT, windowSizeInSeconds, emitFrequencyInSeconds);
    }

    public PartialWindowedRankingsBolt(int topN, long windowSizeInSeconds, long emitFrequencyInSeconds) {
        super(windowSizeInSeconds, emitFrequencyInSeconds);

        if (topN < 1) {
            throw new IllegalArgumentException(String.format("TopN must be >= 1 (you requested %d)", topN));
        }

        this.rankings = new Rankings(topN);
    }

    @Override
    protected void onTick(OutputCollector collector, IClientTimeWindow<Tuple> eventsWindow) {
        Rankings oldRankings = rankings.copy();

        eventsWindow
                .getExpiredEvents()
                .forEach(t -> rankings.removeIfExists(MeanSpeedIntersectionRankable.getInstanceFrom(t)));
        eventsWindow
                .getNewEvents()
                .forEach(t -> rankings.updateWith(MeanSpeedIntersectionRankable.getInstanceFrom(t)));

        if (!rankings.equals(oldRankings) && rankings.size() != 0) {
            collector.emit(new Values(rankings));
        }
    }

    @Override
    protected Long getTimestampFrom(Tuple tuple) {
        return ((MeanSpeedIntersection) tuple.getValueByField(INTERSECTION_MEAN_SPEED_OBJECT)).getOldestSemaphoreTimestamp();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PARTIAL_RANKINGS_OBJECT));
    }

}
