package it.uniroma2.sdcc.trafficcontrol.spout;

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

    private SpoutOutputCollector collector;
    private KafkaConsumer<String, String> consumer;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        Properties props = new Properties();
        props.put(SERVER, KAFKA_IP_PORT);
        props.put(GROUP_ID, APP_NAME);
        props.put(AUTO_COMMIT, TRUE_VALUE);
        props.put(KEY_DESERIALIZER, DESERIALIZER_VALUE);
        props.put(VALUE_DESERIALIZER, DESERIALIZER_VALUE);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(MONITORING_SOURCE));
    }

    @SuppressWarnings("InfiniteLoopStatement")
    @Override
    public void nextTuple() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                // Valore di registrazione della tuple presso il topic kafka
                // TODO TIMESTAMP INUTILE (perchè penso sia quello di quando arriva su kafka
                // TODO mentre a noi serve quello generato dal sensore)
                Long timestamp = record.timestamp();
                // Tupla effettiva (contenente il timestamp relativo alla registrazione effettiva dei valori)
                String tuple = record.value();

                collector.emit(new Values(timestamp, tuple));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(KAFKA_TIMESTAMP, KAFKA_RAW_TUPLE));
    }

}
