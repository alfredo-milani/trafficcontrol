package it.uniroma2.sdcc.trafficcontrol.bolts;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;

public abstract class AbstractKafkaPublisherBolt extends BaseRichBolt {

    private OutputCollector collector;
    private KafkaProducer<String, String> producer;
    private final String topic;

    public AbstractKafkaPublisherBolt(String topic) {
        this.topic = topic;
    }

    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
        props.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        props.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public final void execute(Tuple tuple) {
        try {
            doBefore();
            producer.send(new ProducerRecord<>(topic, computeStringToPublish(tuple)));
            doAfter();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    protected abstract void doBefore();

    protected abstract String computeStringToPublish(Tuple tuple) throws Exception;

    protected abstract void doAfter();

}
