package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.bolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersection;
import lombok.extern.java.Log;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

@Log
public class CongestedIntersectionsPublisherBolt extends AbstractKafkaPublisherBolt<MedianIntersection> {

    public CongestedIntersectionsPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected List<MedianIntersection> computeValueToPublish(Tuple tuple) throws Exception {
        log.info(printArray((ArrayList<MedianIntersection>) tuple));
        return  (ArrayList<MedianIntersection>) tuple;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    private String printArray(ArrayList<MedianIntersection> medianIntersections){
        StringBuilder text= new StringBuilder();
        for (MedianIntersection mi : medianIntersections)
            text.append("Intersection ID: ").append(mi.getIntersectionId())
                    .append("median of the number of vehicle: ").append(mi.getMedianIntersectionCalculated());

        return String.valueOf(text);
    }
}
