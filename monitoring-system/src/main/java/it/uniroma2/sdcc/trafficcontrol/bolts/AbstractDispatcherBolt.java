package it.uniroma2.sdcc.trafficcontrol.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractDispatcherBolt extends BaseRichBolt {

    private OutputCollector collector;
    protected final HashMap<String, Values> streamValueHashMap;

    public AbstractDispatcherBolt() {
        this.streamValueHashMap = new HashMap<>();
    }

    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        try {
            doBefore();

            String stream = computeValuesToEmit(tuple);
            collector.emit(stream, streamValueHashMap.get(stream));

            doAfter();
        } catch (Exception e) {
            System.err.println(String.format("Bad Tuple: %s", tuple.toString()));
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    protected abstract void doBefore();

    protected abstract String computeValuesToEmit(Tuple tuple) throws Exception;

    protected abstract void doAfter();

}
