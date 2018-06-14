package it.uniroma2.sdcc.trafficcontrol.utils;


import java.util.Comparator;

public class RankItemComparator implements Comparator<IntersectionItem> {

    @Override
    public int compare(IntersectionItem i1, IntersectionItem i2) {

        Short averageSpeedI1 = i1.getAverageVehiclesSpeed();
        Short averageSpeedI2 = i2.getAverageVehiclesSpeed();
        // return averageSpeedI1.compareTo(averageSpeedI2);
        int res;
        if (averageSpeedI1 < averageSpeedI2) {
            res = -1;
        } else if (averageSpeedI1 > averageSpeedI2) {
            res = 1;
        } else {
            res = 0;
        }

        return res;
    }



}
