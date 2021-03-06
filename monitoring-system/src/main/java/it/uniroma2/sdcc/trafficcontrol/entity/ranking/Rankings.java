package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.ITupleObject;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.RankingJsonFields.*;

public class Rankings implements ITupleObject {

    private static final int TOP_N_DEFAULT = 10;

    private final int maxSize;
    private final List<IRankable> rankedItems;

    public Rankings() {
        this(TOP_N_DEFAULT);
    }

    public Rankings(int topN) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1");
        }

        this.rankedItems = Lists.newArrayList();
        this.maxSize = topN;
    }

    public Rankings(Rankings other) {
        this(other.maxSize());
        updateWith(other);
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(RANKING_PRINT_TIMESTAMP, System.currentTimeMillis());
        ArrayNode rankingArrayNode = objectNode.putArray(RANKING);
        rankedItems.forEach(r -> {
            ObjectNode rankableNode = mapper.createObjectNode();
            rankableNode.put(MEAN_INTERSECTION_SPEED, r.getValue());
            rankableNode.put(INTERSECTION_ID, (Long) r.getId());
            rankableNode.put(RANKABLE_TIMESTAMP, r.getTimestamp());
            rankingArrayNode.add(rankableNode);
        });

        return objectNode.toString();
    }

    public int maxSize() {
        return maxSize;
    }

    public int size() {
        return rankedItems.size();
    }

    public List<IRankable> getRankings() {
        List<IRankable> copy = Lists.newLinkedList();
        for (int i = 0; i < rankedItems.size(); ++i) {
            copy.add(i, rankedItems.get(i).copy());
        }
        return ImmutableList.copyOf(copy);
    }

    public void updateWith(Rankings other) {
        other.getRankings().forEach(this::updateWith);
    }

    public void updateWith(IRankable r) {
        addOrReplace(r);
        rerank();
        shrinkRankingsIfNeeded();
    }

    public void removeIfExists(Rankings other) {
        rankedItems.removeAll(other.getRankings());
    }

    public void removeIfExists(IRankable r) {
        rankedItems.remove(r);
    }

    private void addOrReplace(IRankable r) {
        Integer rank = findRankOf(r);
        if (rank != null) {
            rankedItems.set(rank, r);
        } else {
            rankedItems.add(r);
        }
    }

    private Integer findRankOf(IRankable r) {
        Object tag = r.getId();
        for (int rank = 0; rank < rankedItems.size(); ++rank) {
            Object cur = rankedItems.get(rank).getId();
            if (cur.equals(tag)) {
                return rank;
            }
        }
        return null;
    }

    private void rerank() {
        Collections.sort(rankedItems);
        Collections.reverse(rankedItems);
    }

    private void shrinkRankingsIfNeeded() {
        if (rankedItems.size() > maxSize) {
            rankedItems.remove(maxSize);
        }
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format(
                "Ranking <print timestamp - %s>\n",
                new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(System.currentTimeMillis()))
        ));
        for (int i = 0; i < rankedItems.size(); ++i) {
            buffer.append(String.format("|%d >\t%s", i + 1, rankedItems.get(i)));
        }

        return buffer.toString();
    }

    public Rankings copy() {
        return new Rankings(this);
    }

    private int getPositionOfLastEqualValue(int initialPosition) {
        int finalIndex = initialPosition;
        Integer currentValue;
        List<IRankable> rankings = Lists.newArrayList(rankedItems);

        if (initialPosition >= rankings.size()) {
            throw new IndexOutOfBoundsException("La posizione iniziale non può essere >= della size");
        }

        try {
            for (; finalIndex < rankings.size(); ++finalIndex) {
                currentValue = rankings.get(finalIndex).getValue();
                if (!currentValue.equals(rankings.get(finalIndex + 1).getValue())) break;
            }
        } catch (IndexOutOfBoundsException e){
            return finalIndex;
        }
        return finalIndex;
    }

    // Due Rankings sono considerati diversi anche se hanno elementi
    // con stesso valore (velocità media) ma posizioni diverse
    @Override
    public boolean equals(Object o) {
        if (o == null) return false;
        if (this == o) return true;
        if (!(o instanceof Rankings)) return false;

        Rankings other = (Rankings) o;

        List<IRankable> thisIRankable = this.getRankings();
        List<IRankable> otherIRankable = other.getRankings();

        if (thisIRankable.size() != otherIRankable.size()) return false;
        if (thisIRankable.size() == 0) return true;

        int currentPositionThis = 0; int finalPositionThis;
        int currentPositionOther = 0; int finalPositionOther;
        do {
            finalPositionThis = this.getPositionOfLastEqualValue(currentPositionThis);
            finalPositionOther = other.getPositionOfLastEqualValue(currentPositionOther);

            List<IRankable> subThis = thisIRankable.subList(currentPositionThis, ++finalPositionThis);
            List<IRankable> subOther = otherIRankable.subList(currentPositionOther, ++finalPositionOther);

            if (subThis.size() != subOther.size()) return false;
            for (IRankable r : subThis) {
                if (!subOther.contains(r)) return false;
            }

            currentPositionThis = finalPositionThis;
            currentPositionOther = finalPositionOther;
        } while (finalPositionThis < thisIRankable.size() && finalPositionOther < otherIRankable.size());

        return true;


        /*
        algoritmo di default

        Rankings other = (Rankings) o;
        return rankedItems.equals(other.getRankings());
        */

        /*
        Algoritmo custom che controlla se ci sono solo due
        successive uguaglianze nel punteggio

        Rankings other = (Rankings) o;
        if (this.size() == other.size()) {
            if (this.size() == 0) return true;

            List<IRankable> l1 = this.getRankings();
            List<IRankable> l2 = other.getRankings();
            int i = 0;
            do {
                // VALIDO SOLO PER 2 ELEMENTI IN SUCCESSIONE
                if (!l1.get(i).equals(l2.get(i))) {
                    try {
                        if (!(l1.get(i).equals(l2.get(i + 1)) &&
                                l1.get(i + 1).equals(l2.get(i)))) {
                            return false;
                        } else {
                            ++i;
                        }
                    } catch (IndexOutOfBoundsException e) {
                        return false;
                    }
                }
                ++i;
            } while (i < this.size());
            return true;
        } else {
            return false;
        }
        */
    }

    @Override
    public int hashCode() {
        return rankedItems.hashCode();
    }

}
