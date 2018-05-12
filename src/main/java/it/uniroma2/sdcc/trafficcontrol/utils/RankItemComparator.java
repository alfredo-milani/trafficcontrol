package it.uniroma2.sdcc.trafficcontrol.utils;

import java.util.Comparator;

/**
 * Created by matteo on 17/03/17.
 */
public class RankItemComparator implements Comparator<RankItem> {

    public int compare(RankItem r1, RankItem r2) {
        long currentTime = System.currentTimeMillis();

        Long expectedTTL1 = r1.getInstallationTimestamp() + r1.getMeanExpirationTime();
        Long expectedTTL2 = r2.getInstallationTimestamp() + r2.getMeanExpirationTime();

        return expectedTTL1.compareTo(expectedTTL2);
    }
}
