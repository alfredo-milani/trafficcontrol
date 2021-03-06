package it.uniroma2.sdcc.trafficcontrol.abstractsBolts;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;

public abstract class AbstractKafkaPublisherBolt<V> extends BaseRichBolt {

    private OutputCollector collector;
    private KafkaProducer<String, V> producer;
    private final String topic;
    // File di configurazione
    protected final AppConfig appConfig;

    public AbstractKafkaPublisherBolt(String topic) {
        this(AppConfig.getInstance(), topic);
    }

    public AbstractKafkaPublisherBolt(AppConfig appConfig, String topic) {
        this.appConfig = appConfig;
        this.topic = topic;
    }

    @Override
    public final void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, appConfig.getKafkaIpPort());
        props.put(KEY_SERIALIZER, SERIALIZER_VALUE);
        props.put(VALUE_SERIALIZER, SERIALIZER_VALUE);

        producer = new KafkaProducer<>(props);
        this.collector = collector;
    }

    @Override
    public final void execute(Tuple tuple) {
        try {
            computeValueToPublish(tuple).forEach(v -> producer.send(new ProducerRecord<>(topic, v)));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    protected abstract @NotNull List<V> computeValueToPublish(Tuple tuple) throws Exception;

}
