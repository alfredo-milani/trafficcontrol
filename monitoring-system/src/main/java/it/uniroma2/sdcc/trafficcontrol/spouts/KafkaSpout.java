package it.uniroma2.sdcc.trafficcontrol.spouts;

import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;

public class KafkaSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    private final String sourceTopic;
    private final String groupId;
    // File di configurazione
    private final AppConfig appConfig;

    public KafkaSpout(AppConfig appConfig, String sourceTopic) {
        this(appConfig, sourceTopic, appConfig.getApplicationName());
    }

    public KafkaSpout(AppConfig appConfig, String sourceTopic, String groupId) {
        this.sourceTopic = sourceTopic;
        this.groupId = groupId;
        this.appConfig = appConfig;
    }

    @Override
    public final void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, appConfig.getKafkaIpPort());
        props.put(GROUP_ID, groupId);
        props.put(AUTO_COMMIT, TRUE_VALUE);
        props.put(KEY_DESERIALIZER, DESERIALIZER_VALUE);
        props.put(VALUE_DESERIALIZER, DESERIALIZER_VALUE);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(sourceTopic));
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public final void nextTuple() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // Timestamp della tupla nel topic
                Long timestamp = record.timestamp();
                // Valore tupla
                String tuple = record.value();

                collector.emit(new Values(timestamp, tuple));
            }
        }
    }

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(KAFKA_TIMESTAMP, KAFKA_RAW_TUPLE));
    }

}
