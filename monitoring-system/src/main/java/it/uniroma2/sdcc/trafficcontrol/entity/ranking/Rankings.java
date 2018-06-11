package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Rankings implements Serializable {

    private final static ObjectMapper mapper = new ObjectMapper();

    private static final int DEFAULT_TOP_N = 10;

    private final int maxSize;
    private final List<IRankable> rankedItems = Lists.newArrayList();

    public Rankings() {
        this(DEFAULT_TOP_N);
    }

    public Rankings(int topN) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1");
        }

        this.maxSize = topN;
    }

    public Rankings(Rankings other) {
        this(other.maxSize());
        updateWith(other);
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
        List<IRankable> rankables = other.getRankings();
        rankables.forEach(this::removeIfExists);
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

    public void pruneZeroCounts() {
        int i = 0;
        while (i < rankedItems.size()) {
            if (rankedItems.get(i).getValue() == 0) {
                rankedItems.remove(i);
            } else {
                ++i;
            }
        }
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format("Rannking <%d - timestamp>\n", System.currentTimeMillis()));
        for (int i = 0; i < rankedItems.size(); ++i) {
            buffer.append(String.format("|%d >\t%s", i + 1, rankedItems.get(i)));
        }
        buffer.append("\n");

        return buffer.toString();
    }

    public Rankings copy() {
        return new Rankings(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Rankings)) {
            return false;
        }

        Rankings other = (Rankings) o;
        return rankedItems.equals(other.getRankings());
    }

}
