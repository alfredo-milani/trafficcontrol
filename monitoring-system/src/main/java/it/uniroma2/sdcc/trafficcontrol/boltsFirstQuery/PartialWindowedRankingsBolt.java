package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.MeanSpeedIntersection;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.MeanSpeedIntersectionRankable;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
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
        Rankings oldRankings = rankings.copy();

        eventsWindow
                .getExpiredEventsWindow()
                .forEach(t -> rankings.removeIfExists(MeanSpeedIntersectionRankable.getInstanceFrom(t)));
        eventsWindow
                .getNewEventsWindow()
                .forEach(t -> rankings.updateWith(MeanSpeedIntersectionRankable.getInstanceFrom(t)));

        if (!oldRankings.equals(rankings) && rankings.size() != 0) {
            collector.emit(new Values(rankings));
        }
    }

    @Override
    protected void onValidTupleReceived(Tuple tuple) {

    }

    @Override
    protected Long getTimestampFrom(Tuple tuple) {
        MeanSpeedIntersection meanSpeedIntersection = (MeanSpeedIntersection) tuple.getValueByField(INTERSECTION_MEAN_SPEED_OBJECT);
        return meanSpeedIntersection.getOldestSemaphoreTimestam();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(PARTIAL_RANKINGS_OBJECT));
    }

}
