package it.uniroma2.sdcc.trafficcontrol.utils;


import java.io.Serializable;
import java.util.ArrayList;


public class Ranking implements Serializable {

    private ArrayList<IntersectionItem> ranking;

    public ArrayList<IntersectionItem> getRanking() {
        return ranking;
    }

    public void setRanking(ArrayList<IntersectionItem> ranking) {
        this.ranking = ranking;
    }
}
