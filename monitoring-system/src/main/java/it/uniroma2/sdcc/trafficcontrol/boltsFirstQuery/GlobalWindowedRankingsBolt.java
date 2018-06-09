package it.uniroma2.sdcc.trafficcontrol.boltsFirstQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractWindowedBolt;
import it.uniroma2.sdcc.trafficcontrol.bolts.IWindow;
import it.uniroma2.sdcc.trafficcontrol.entity.ranking.Rankings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.GLOBAL_RANKINGS_OBJECT;
import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.PARTIAL_RANKINGS_OBJECT;

public class GlobalWindowedRankingsBolt extends AbstractWindowedBolt {

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
        Rankings oldRankings = rankings.copy(false);

        eventsWindow.getExpiredEventsWindow().forEach(t -> {
            Rankings rankings = (Rankings) t.getValueByField(PARTIAL_RANKINGS_OBJECT);
            this.rankings.removeIfExists(rankings);
        });
        eventsWindow.getNewEventsWindow().forEach(t -> {
            Rankings rankings = (Rankings) t.getValueByField(PARTIAL_RANKINGS_OBJECT);
            this.rankings.updateWith(rankings);
        });

        if (!oldRankings.equals(rankings)) {
            collector.emit(new Values(rankings));
        }

        eventsWindow.getExpiredEventsWindow().forEach(t -> {
            Rankings rankings = (Rankings) t.getValueByField(PARTIAL_RANKINGS_OBJECT);
            System.out.println(rankings.toString());
        });
    }

    @Override
    protected void onTupleReceived(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(GLOBAL_RANKINGS_OBJECT));
    }

    // TODO i partial bolt devo sempre emettere tuple? Perché, nel caso in cui la posizione nella classifica
    // locale non cambia, ma cambia solo il valore della media, nella classifica globale potrebbe cambiare la posizione
    // in classifica.

    // TODO anche qui i valori expired devono essere eliminati dalla classifica locale (quindi anche questo bolt
    // deve avere finesrtre temporali) altrimenti valori vecchi (scaduti dopo i 15 minuti) saranno inviati al
    // global bolt anche se nella sua finestra temporale sono usciti e quindi non andrebbero considerati

    // TODO SISTEMARE NULLPOINTER EXCEPTION in removeIfExistsThreadSafe(rankins) -> rankedItems.stream().map(Rankable::copy).forEach(copy::add);

}
