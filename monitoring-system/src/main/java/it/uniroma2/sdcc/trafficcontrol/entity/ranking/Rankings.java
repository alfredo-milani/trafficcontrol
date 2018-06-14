package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Rankings implements Serializable {

    private static final int TOP_N_DEFAULT = 10;

    private final int maxSize;
    private final List<IRankable> rankedItems = Lists.newArrayList();

    public Rankings() {
        this(TOP_N_DEFAULT);
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
        /*synchronized (rankedItems) {
            other.getRankings().forEach(this::updateWith);
        }*/
        other.getRankings().forEach(this::updateWith);
    }

    public void updateWith(IRankable r) {
        /*synchronized (rankedItems) {
            addOrReplace(r);
            rerank();
            shrinkRankingsIfNeeded();
        }*/
        addOrReplace(r);
        rerank();
        shrinkRankingsIfNeeded();
    }

    public void removeIfExists(Rankings other) {
        /*synchronized (rankedItems) {
            // other.getRankings().forEach(this::removeIfExists);
            rankedItems.removeAll(other.getRankings());
        }*/
        // other.getRankings().forEach(this::removeIfExists);
        rankedItems.removeAll(other.getRankings());
    }

    public void removeIfExists(IRankable r) {
        /*synchronized (rankedItems) {
            rankedItems.remove(r);
        }*/
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

        /*Rankings other = (Rankings) o;
        if (this.size() == other.size()) {
            List<IRankable> otherList = other.getRankings();
            for (int i = 0; i < this.size(); ++i) {
                if (!this.rankedItems.get(i).equals(otherList.get(i))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }*/
    }

    public static void main(String[] a) {
        Rankings r1 = new Rankings(10);
        Rankings r2 = new Rankings(10);

        MeanSpeedIntersectionRankable m1 = new MeanSpeedIntersectionRankable(12L, 23, 324351535L);
        MeanSpeedIntersectionRankable m2 = new MeanSpeedIntersectionRankable(14L, 44, 321234335L);
        MeanSpeedIntersectionRankable m3 = new MeanSpeedIntersectionRankable(18L, 61, 324111535L);
        MeanSpeedIntersectionRankable m4 = new MeanSpeedIntersectionRankable(12L, 23, 324351535L);

        r1.updateWith(m1);
        r1.updateWith(m2);

        r2.updateWith(m3);
        r2.updateWith(m4);

        r1.getRankings().forEach(System.out::println);
        r2.getRankings().forEach(System.out::println);

        r1.removeIfExists(r2);

        r1.getRankings().forEach(System.out::println);
    }

}
