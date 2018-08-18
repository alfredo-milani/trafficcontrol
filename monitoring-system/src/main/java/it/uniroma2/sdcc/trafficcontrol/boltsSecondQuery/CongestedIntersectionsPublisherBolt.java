package it.uniroma2.sdcc.trafficcontrol.boltsSecondQuery;

import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractKafkaPublisherBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.MedianIntersectionManager;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static it.uniroma2.sdcc.trafficcontrol.constants.StormParams.CONGESTED_INTERSECTIONS;

public class CongestedIntersectionsPublisherBolt extends AbstractKafkaPublisherBolt<String> {

    public CongestedIntersectionsPublisherBolt(String topic) {
        super(topic);
    }

    @Override
    protected List<String> computeValueToPublish(Tuple tuple) {
        MedianIntersectionManager medianIntersectionManager = (MedianIntersectionManager) tuple.getValueByField(CONGESTED_INTERSECTIONS);
        return new ArrayList<>(Collections.singletonList(medianIntersectionManager.toString()));
        // return new ArrayList<>(Collections.singletonList(medianIntersectionManager.getJsonStringFromInstance()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
