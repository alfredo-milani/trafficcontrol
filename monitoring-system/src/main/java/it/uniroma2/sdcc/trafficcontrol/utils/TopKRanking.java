package it.uniroma2.sdcc.trafficcontrol.utils;


import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;


public class TopKRanking {
    private static final int NOT_PRESENT = -1;
    private Comparator<IntersectionItem> comparator = null;
    private ArrayList<IntersectionItem> ranking = null;
    private int topK;

    public TopKRanking(int k) {
        this.comparator = new RankItemComparator();
        this.ranking = new ArrayList<IntersectionItem>();
        this.topK = k;
    }

    public boolean update(IntersectionItem item) { //true if update needed or false

        int sizePreUpdate = ranking.size();
        int oldPosition = indexOf(item); //TUTTO BENE

        if (oldPosition != NOT_PRESENT) {
            ranking.remove(item);
        }

        int newPosition = add(item); // new item position

        int sizePostUpdate = ranking.size(); // new ranking size

        if (newPosition == oldPosition &&
                sizePreUpdate == sizePostUpdate) {

			/* do not notify position changed */
            return false;

        } else return newPosition <= topK - 1;

    }

    public int add(IntersectionItem item) {
        int pos = Collections.binarySearch(ranking, item, comparator);
        int insertionPoint = pos > -1 ? pos : -pos - 1;
        if (insertionPoint < 0)
            ranking.add(-insertionPoint - 1, item);
        else
            ranking.add(insertionPoint, item);
        return insertionPoint;
    }

    public boolean remove(IntersectionItem item) {
        return ranking.remove(item);
    }

    public int indexOf(IntersectionItem item) {
        for (int i = 0; i < ranking.size(); i++) {
            if (item.equals(ranking.get(i)))
                return i;
        }
        return NOT_PRESENT;
    }

    public boolean containsElement(IntersectionItem item) {
        return ranking.contains(item);
    }

    public Ranking getTopK() {

        ArrayList<IntersectionItem> top = new ArrayList<IntersectionItem>();

        if (ranking.isEmpty()) {
            Ranking topKRanking = new Ranking();
            topKRanking.setRanking(top);
            return topKRanking;
        }

        int elems = Math.min(topK, ranking.size());

        for (int i = 0; i < elems; i++) {
            top.add(ranking.get(i));
        }

        Ranking topKRanking = new Ranking();
        topKRanking.setRanking(top);
        return topKRanking;

    }

    @Override
    public String toString() {
        return ranking.toString();
    }

    //aggiunta per seconda query
    public ArrayList<IntersectionItem> returnArray(){
        return ranking;
    }

}
