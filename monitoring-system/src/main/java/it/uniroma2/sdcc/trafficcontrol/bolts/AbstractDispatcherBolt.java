package it.uniroma2.sdcc.trafficcontrol.bolts;

import com.sun.tools.javac.util.Assert;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadStream;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public abstract class AbstractDispatcherBolt extends BaseRichBolt {

    protected final static String DEFAULT_STREAM = "default_stream";

    private OutputCollector collector;

    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        try {
            Map<String, Values> streamValueMap = declareStreamValue(tuple);
            Assert.checkNonNull(streamValueMap);

            if (streamValueMap.get(DEFAULT_STREAM) != null) {
                if (streamValueMap.size() > 1) {
                    throw new BadStream("Non possono essere emessi dati contemporaneamente sullo stream di default e su altri streams");
                }
                collector.emit(streamValueMap.get(DEFAULT_STREAM));
            } else {
                streamValueMap.keySet().forEach(s -> collector.emit(s, streamValueMap.get(s)));
            }
        } catch (BadTuple e) {
            System.err.println(String.format("Bad Tuple: %s", tuple.toString()));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    /**
     * Computa la stringa da emettere verso il bolt successivo nella topologia.
     *
     * @param tuple Tupla ricevuta dal bolt precedente
     * Se si vuole usare lo stream di default utilizzare la key {@link AbstractDispatcherBolt#DEFAULT_STREAM}
     * @throws Exception Generica eccezione
     */
    protected abstract Map<String, Values> declareStreamValue(Tuple tuple) throws Exception;

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        Map<String, Fields> streamFieldMap = declareStreamField();
        Assert.checkNonNull(streamFieldMap);

        try {
            if (streamFieldMap.get(DEFAULT_STREAM) != null) {
                if (streamFieldMap.size() > 1) {
                    throw new BadStream("Non possono essere emessi dati contemporaneamente sullo stream di default e su altri streams");
                }
                declarer.declare(streamFieldMap.get(DEFAULT_STREAM));
            } else {
                streamFieldMap.keySet().forEach(s -> declarer.declareStream(s, streamFieldMap.get(s)));
            }
        } catch (BadStream e) {
            e.printStackTrace();
        }
    }

    /**
     * Computa la stringa da emettere verso il bolt successivo nella topologia.
     * In questo metodo è preferibile popolare l'hashMap che mappa gli streams con i campi da emettere.
     *
     * Se si vuole usare lo stream di default utilizzare la key {@link AbstractDispatcherBolt#DEFAULT_STREAM}
     */
    protected abstract Map<String, Fields> declareStreamField();

}
