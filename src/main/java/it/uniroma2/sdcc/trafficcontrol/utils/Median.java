package it.uniroma2.sdcc.trafficcontrol.utils;

import static it.uniroma2.sdcc.trafficcontrol.constants.Constants.*;

public class Median {
    private PQuantile pQuantile;
    private double[] n;
    private int size;
    private String address;

    public Median() {

        this.pQuantile = null;
        this.size = 0;
        this.n = new double[PP_CONSTANT];
        this.address = "";

    }


    public Median(String address) {

        this.pQuantile = null;
        this.size = 0;
        this.n = new double[PP_CONSTANT];
        this.address = address;

    }

    public double observer(double consume) {
        if (size < PP_CONSTANT) {
            n[size] = consume;
            size++;
        }

        if (size == PP_CONSTANT && pQuantile == null) {
            pQuantile = new PQuantile(MEDIAN, n[0], n[1], n[2], n[3], n[4]);
            return pQuantile.estimatedPQuantile();
        } else if (size == PP_CONSTANT && pQuantile != null) {
            pQuantile.observation(consume);
            return pQuantile.estimatedPQuantile();
        }
        return NOT_FOUND;
    }


    public double publish() {

        if (pQuantile == null) {
            pQuantile = new PQuantile(MEDIAN, n[0], n[1], n[2], n[3], n[4]);
        }

        if (size == 0) {
            return NOT_FOUND;
        }

        if (size < PP_CONSTANT) {
            return NOT_FOUND;
        }
        return pQuantile.estimatedPQuantile();
    }
}
