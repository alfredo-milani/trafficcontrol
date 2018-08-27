import it.uniroma2.sdcc.trafficcontrol.topologies.FirstTopology;
import it.uniroma2.sdcc.trafficcontrol.utils.ApplicationsProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.RANKING_15_MIN;

public class InfluxDBClient {

    // Connect to InfluxDB
    private final InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "influxTest", "pass");
    private final ApplicationsProperties properties;
    private final String dbName = FirstTopology.class.getSimpleName();

    public InfluxDBClient() throws IOException {
        properties = ApplicationsProperties.getInstance();
        properties.loadProperties();
        // Creating Database
        influxDB.createDatabase(dbName);
    }

    public void populateInfluxDB() {
        String sourceString = "";
        // LampKafkaConsumer lampKafkaConsumer;

        Properties props = new Properties();
        props.put("bootstrap.servers", ApplicationsProperties.KAFKA_IP_PORT);
        props.put("group.id", "READERS");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(RANKING_15_MIN));

        //consume data from Kafka
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                sourceString = record.value();

                /*try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }*/

                // Creation and definition of a batch containing points for influxDB
                BatchPoints batchPoints = BatchPoints
                        .database(dbName)
                        .retentionPolicy("autogen")
                        .consistency(InfluxDB.ConsistencyLevel.ALL)
                        .build();

                Point point1 = Point.measurement("Ranking")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("1", 32)
                        .build();
                Point point2 = Point.measurement("Ranking")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("2", 54)
                        .build();
                Point point3 = Point.measurement("Ranking")
                        .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                        .addField("3", 75)
                        .build();

                batchPoints.point(point1);
                batchPoints.point(point2);
                batchPoints.point(point3);

                // Write on influxDB
                influxDB.write(batchPoints);
            }
        }

    }

    public static void main(String[] args) throws IOException {
        InfluxDBClient e = new InfluxDBClient();
        e.populateInfluxDB();
    }

}
