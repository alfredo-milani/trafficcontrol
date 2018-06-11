package it.uniroma2.sdcc.trafficcontrol.entity.ranking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Rankings implements Serializable {

    private final static ObjectMapper mapper = new ObjectMapper();

    private static final int DEFAULT_TOP_N = 10;

    private final int maxSize;
    private final List<Rankable> rankedItems = Lists.newArrayList();

    public Rankings() {
        this(DEFAULT_TOP_N);
    }

    public Rankings(int topN) {
        if (topN < 1) {
            throw new IllegalArgumentException("topN must be >= 1");
        }

        this.maxSize = topN;
    }

    /**
     * Copy constructor.
     *
     * @param other
     */
    public Rankings(Rankings other, boolean threadSafety) {
        this(other.maxSize());

        if (threadSafety) {
            updateWithThreadSafe(other);
        } else {
            updateWith(other);
        }
    }

    /**
     * @return the maximum possible number (size) of ranked objects this instance can hold
     */
    public int maxSize() {
        return maxSize;
    }

    /**
     * @return the number (size) of ranked objects this instance is currently holding
     */
    public int size() {
        return rankedItems.size();
    }

    /**
     * The returned defensive copy is only "somewhat" defensive.  We do, for instance, return a defensive copy of the
     * enclosing List instance, and we do try to defensively copy any contained Rankable objects, too.  However, the
     * contract of {link org.apache.storm.starter.tools.Rankable#copy()} does not guarantee that any Object's embedded within
     * a Rankable will be defensively copied, too.
     *
     * @return a somewhat defensive copy of ranked items
     */
    public List<Rankable> getRankingsThreadSafe() {
        synchronized (rankedItems) {
            List<Rankable> copy = Lists.newLinkedList();
            rankedItems.stream().map(Rankable::copy).forEach(copy::add);
            return ImmutableList.copyOf(copy);
        }
    }

    public List<Rankable> getRankings() {
        List<Rankable> copy = Lists.newLinkedList();
        for (int i = 0; i < rankedItems.size(); ++i) {
            copy.add(i, rankedItems.get(i).copy());
        }
        /*for (Rankable r : rankedItems) {
            copy.add(r.copy());
        }*/
        return ImmutableList.copyOf(copy);
    }

    public void updateWithThreadSafe(Rankings other) {
        synchronized (rankedItems) {
            other.getRankings().forEach(this::updateWithThreadSafe);
        }
    }

    public void updateWith(Rankings other) {
        for (Rankable r : other.getRankings()) {
            updateWith(r);
        }
    }

    public void updateWithThreadSafe(Rankable r) {
        synchronized (rankedItems) {
            addOrReplace(r);
            rerank();
            shrinkRankingsIfNeeded();
        }
    }

    public void updateWith(Rankable r) {
        addOrReplace(r);
        rerank();
        shrinkRankingsIfNeeded();
    }

    public void removeIfExistsThreadSafe(Rankings other) {
        synchronized (rankedItems) {
            other.getRankings().forEach(this::removeIfExistsThreadSafe);
        }
    }

    public void removeIfExists(Rankings other) {
        List<Rankable> rankables = other.getRankings();
        for (Rankable r : rankables) {
            removeIfExists(r);
        }
    }

    public boolean removeIfExistsThreadSafe(Rankable r) {
        synchronized (rankedItems) {
            return rankedItems.remove(r);
        }
    }

    public void removeIfExists(Rankable r) {
        rankedItems.remove(r);
    }

    private void addOrReplace(Rankable r) {
        Integer rank = findRankOf(r);
        if (rank != null) {
            rankedItems.set(rank, r);
        } else {
            rankedItems.add(r);
        }
    }

    private Integer findRankOf(Rankable r) {
        Object tag = r.getObject();
        for (int rank = 0; rank < rankedItems.size(); ++rank) {
            Object cur = rankedItems.get(rank).getObject();
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

    /**
     * Removes ranking entries that have a count of zero.
     */
    public void pruneZeroCountsThreadSafe() {
        synchronized (rankedItems) {
            int i = 0;
            while (i < rankedItems.size()) {
                if (rankedItems.get(i).getMeanIntersectionSpeed() == 0) {
                    rankedItems.remove(i);
                } else {
                    i++;
                }
            }
        }
    }

    public void pruneZeroCounts() {
        int i = 0;
        while (i < rankedItems.size()) {
            if (rankedItems.get(i).getMeanIntersectionSpeed() == 0) {
                rankedItems.remove(i);
            } else {
                i++;
            }
        }
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format("%d\tRANKING:\n", System.currentTimeMillis()));
        for (int i = 0; i < rankedItems.size(); ++i) {
            buffer.append(String.format("/ %d \\\t%s\n", i + 1, rankedItems.get(i)));
        }
        buffer.append("\n");

        return buffer.toString();
    }

    public String getJsonFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        for (int p = 0; p < rankedItems.size(); ++p) {
            objectNode.put(String.valueOf(p + 1), rankedItems.toString());
        }

        return objectNode.toString();
    }

    /**
     * Creates a (defensive) copy of itself.
     */
    public Rankings copy(boolean threadSafety) {
        return new Rankings(this, threadSafety);
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

    public static void main(String[] a) {
        /*IntersectionRankable intersectionRankable1 = new IntersectionRankable(12L, 32);
        IntersectionRankable intersectionRankable2 = new IntersectionRankable(13L, 43);
        IntersectionRankable intersectionRankable3 = new IntersectionRankable(111L, 65);

        Rankings rankings = new Rankings();
        rankings.updateWith(intersectionRankable1);
        rankings.updateWith(intersectionRankable2);
        rankings.updateWith(intersectionRankable3);

        IntersectionRankable intersectionRankable4 = new IntersectionRankable(12L, 32);
        IntersectionRankable intersectionRankable5 = new IntersectionRankable(13L, 43);
        IntersectionRankable intersectionRankable6 = new IntersectionRankable(111L, 60);

        Rankings rankings1 = new Rankings();
        rankings1.updateWith(intersectionRankable4);
        rankings1.updateWith(intersectionRankable6);
        rankings1.updateWith(intersectionRankable5);

        // System.out.println("REM: " + o.remove(r));
        rankings.getRankings().forEach(all -> System.out.println("R1: " + all.toString()));
        rankings1.getRankings().forEach(all -> System.out.println("R2: " + all.toString()));

        System.out.println("DIO " + rankings.equals(rankings1));

        rankings.removeIfExists(rankings1);
        rankings.getRankings().forEach(all -> System.out.println("R1: " + all.toString()));
        rankings1.getRankings().forEach(all -> System.out.println("R2: " + all.toString()));*/

        /*Rankings r1 = new Rankings(10);
        Rankings r2 = new Rankings(10);
        System.out.println("R1: " + r1.hashCode() + "\tR: " + r1.copy(false).hashCode());*/
    }

}
