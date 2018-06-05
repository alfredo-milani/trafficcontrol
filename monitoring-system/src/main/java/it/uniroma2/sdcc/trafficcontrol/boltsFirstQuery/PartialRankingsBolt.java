package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.entity.ranking.IntersectionRankable;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankable;
import org.apache.storm.tuple.Tuple;

import java.util.logging.Logger;

public class PartialRankingsBolt extends AbstractRankerBolt {

    private final static String CLASS_NAME = PartialRankingsBolt.class.getSimpleName();
    private final static Logger LOGGER = Logger.getLogger(CLASS_NAME);

    public PartialRankingsBolt() {
        super();
    }

    public PartialRankingsBolt(int topN) {
        super(topN);
    }

    public PartialRankingsBolt(int topN, int emitFrequencyInSeconds) {
        super(topN, emitFrequencyInSeconds);
    }

    @Override
    void updateRankingsWithTuple(Tuple tuple) {
        Rankable rankable = IntersectionRankable.from(tuple);
        super.getRankings().updateWith(rankable);
    }

    @Override
    public Logger getLogger() {
        return LOGGER;
    }

}
