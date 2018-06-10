package it.uniroma2.sdcc.trafficcontrol.spouts;

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

import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.APP_NAME;
import static it.uniroma2.sdcc.trafficcontrol.constants.InputParams.KAFKA_IP_PORT;
import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.*;


public class KafkaSpout extends BaseRichSpout {

    private final static String DEFAULT_GROUP_ID = APP_NAME;

    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;
    private final String sourceTopic;
    private final String groupId;

    public KafkaSpout(String sourceTopic) {
        this(sourceTopic, DEFAULT_GROUP_ID);
    }

    public KafkaSpout(String sourceTopic, String groupId) {
        this.sourceTopic = sourceTopic;
        this.groupId = groupId;
    }

    @Override
    public final void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS, KAFKA_IP_PORT);
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
