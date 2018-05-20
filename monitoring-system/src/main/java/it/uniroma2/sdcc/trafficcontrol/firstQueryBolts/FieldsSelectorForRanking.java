package it.uniroma2.sdcc.trafficcontrol.firstQueryBolts;

import it.uniroma2.sdcc.trafficcontrol.topology.FirstTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.logging.Level;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.*;


public class FieldsSelectorForRanking extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        collector.ack(tuple);

        // Filtro informazioni sensore per elaborare la classifica
        Long intersectionId = tuple.getLongByField(INTERSECTION_ID);
        Long semaphoreId = tuple.getLongByField(SEMAPHORE_ID);
        Double semaphoreLatitude = tuple.getDoubleByField(SEMAPHORE_LATITUDE);
        Double semaphoreLongitude = tuple.getDoubleByField(SEMAPHORE_LONGITUDE);
        Short averageVehiclesSpeed = tuple.getShortByField(AVERAGE_VEHICLES_SPEED);

        Values values = new Values(
                intersectionId,
                semaphoreId,
                semaphoreLatitude,
                semaphoreLongitude,
                averageVehiclesSpeed
        );

        FirstTopology.getLOGGER().log(Level.INFO, values.toString());

        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                INTERSECTION_ID,
                SEMAPHORE_ID,
                SEMAPHORE_LATITUDE,
                SEMAPHORE_LONGITUDE,
                AVERAGE_VEHICLES_SPEED
        ));
    }

}
