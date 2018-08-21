package it.uniroma2.sdcc.trafficcontrol.entity;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.tdunning.math.stats.TDigest;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.ITupleObject;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import lombok.Getter;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.text.SimpleDateFormat;
import java.util.*;

import static it.uniroma2.sdcc.trafficcontrol.constants.MedianVehiclesIntersectionJsonFields.*;

public class MedianIntersectionManager implements ITupleObject {

    public final static double COMPRESSION = 100;
    public final static double QUANTILE = 0.5; // mediana

    private final Map<Long, MedianIntersection> higherMedianIntersection;
    @Getter
    private double globalMedian;

    public MedianIntersectionManager() {
        higherMedianIntersection = new LinkedHashMap<>();
        globalMedian = 0.0;
    }

    public void updateMedianFrom(Map<Long, RichSemaphoreSensor> richSemaphoreSensorMap) {
        globalMedian = computeMedian(getDoubleArrayFromSemaphore(Lists.newArrayList(richSemaphoreSensorMap.values())));
        // Eliminazione intersezioni la cui mediana è minore rispetto la nuova mediana globale calcolata
        higherMedianIntersection.values().removeIf(i -> i.getMedianIntersection() <= globalMedian);
    }

    public void addIntersectionIfNeeded(MedianIntersection medianIntersectionToAdd) {
        if (medianIntersectionToAdd.getMedianIntersection() > globalMedian) {
            higherMedianIntersection.put(medianIntersectionToAdd.getIntersectionId(), medianIntersectionToAdd);
        }
    }

    private Double computeMedian(Double... values) {
        TDigest tDigestIntesection = TDigest.createAvlTreeDigest(COMPRESSION);
        for (Double v : values) {
            tDigestIntesection.add(v);
        }
        return tDigestIntesection.quantile(QUANTILE);
    }

    private Double[] getDoubleArrayFromSemaphore(List<RichSemaphoreSensor> richSemaphoreSensors) {
        Double[] values = new Double[richSemaphoreSensors.size()];
        for (int i = 0; i < richSemaphoreSensors.size(); ++i) {
            values[i] = Double.valueOf(richSemaphoreSensors.get(i).getVehiclesNumber());
        }
        return values;
    }

    /**
     * Defensive copy of {@link MedianIntersectionManager#higherMedianIntersection}
     *
     * @return Map of intersection with median higher then global median
     */
    public Map<Long, MedianIntersection> getHigherMedianIntersection() {
        return new HashMap<>(higherMedianIntersection);
    }

    public boolean remove(Long id, MedianIntersection medianIntersection) {
        return higherMedianIntersection.remove(id, medianIntersection);
    }

    public List<MedianIntersection> getListOfHigherMedianIntersection() {
        return Lists.newArrayList(higherMedianIntersection.values());
    }

    @SuppressWarnings("Duplicates")
    public void sort() {
        List<MedianIntersection> toSort = Lists.newArrayList(higherMedianIntersection.values());
        toSort.sort((o1, o2) -> {
            double delta = o1.getMedianIntersection() - o2.getMedianIntersection();
            if (delta > 0) return -1;
            else if (delta == 0) return 0;
            else return 1;
        });

        higherMedianIntersection.clear();
        toSort.forEach(i -> higherMedianIntersection.put(i.getIntersectionId(), i));
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append(String.format(
                "Higher median intersections <print timestamp - %s>\n[Current global median: %.2f vehicles]\n",
                new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(System.currentTimeMillis())),
                globalMedian
        ));
        higherMedianIntersection.values().forEach(
                i -> buffer.append(String.format("| Id: %d >\tvehicles: %.2f\n", i.getIntersectionId(), i.getMedianIntersection()))
        );
        buffer.append("\n");

        return buffer.toString();
    }

    @Override
    public String getJsonStringFromInstance() {
        ObjectNode objectNode = mapper.createObjectNode();

        objectNode.put(HIGHER_MEDIAN_PRINT_TIMESTAMP, System.currentTimeMillis());
        objectNode.put(GLOBAL_MEDIAN, globalMedian);
        ArrayNode rankingArrayNode = objectNode.putArray(HIGHER_MEDIAN);
        higherMedianIntersection.values().forEach(i -> {
            ObjectNode intersection = mapper.createObjectNode();
            intersection.put(MEDIAN_VEHICLES_INTERSECTION, i.getMedianIntersection());
            intersection.put(INTERSECTION_ID, i.getIntersectionId());
            intersection.put(INTERSECTION_TIMESTAMP, i.getOldestSemaphoreTimestamp());
            rankingArrayNode.add(intersection);
        });

        return objectNode.toString();
    }

}
