package it.uniroma2.sdcc.trafficcontrol.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static it.uniroma2.sdcc.trafficcontrol.constants.TupleFields.*;


public class ParserBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        try {
            Pattern pattern = Pattern.compile("[^0-9]*([0-9]+).*'(.*?)'.*'(.*?)'.*=[^0-9]*([0-9]+).*'(.*?)'.*=[^0-9]*([0-9]+).*=[^0-9]*([0-9]+).*=[^0-9]*([0-9]+).*=[^0-9]*([0-9]+).*'(.*?)'");
            Matcher matcher = pattern.matcher(tuple.getStringByField(RAW_TUPLE));
            System.out.println("TTT: " + tuple);
            System.out.println("MMM: " + matcher.groupCount());
            for (int i = 0; i < matcher.groupCount(); ++i) {
                System.out.println("MATCH: " + matcher.group(i));
            }

            Long t = tuple.getLongByField(TIMESTAMP);
            Integer i = Integer.parseInt(matcher.group(1));
            String c = matcher.group(2);
            System.out.println("TIME: " + t + "\tID: " + i + "\tCITY: " + c);


            if (matcher.find()) {

                Long timestamp = tuple.getLongByField(TIMESTAMP);

                Integer id = Integer.parseInt(matcher.group(1));
                String city = matcher.group(2), address = matcher.group(3);
                Integer km = Integer.parseInt(matcher.group(4));

                String bulbModel = matcher.group(5);
                Double maxWatts = Double.parseDouble(matcher.group(6)), currentWatts = Double.parseDouble(matcher.group(9));
                Long installationTimestamp = Long.parseLong(matcher.group(7)), meanExpirationTime = Long.parseLong(matcher.group(8));
                Boolean state = Boolean.parseBoolean(matcher.group(10));

                Values values = new Values(
                        timestamp,
                        id,
                        city,
                        address,
                        km,
                        bulbModel,
                        maxWatts,
                        currentWatts,
                        installationTimestamp,
                        meanExpirationTime,
                        state
                );

                collector.emit(values);
            } else {
                throw new Exception();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TIMESTAMP, ID, CITY, ADDRESS, KM, BULB_MODEL, MAX_WATTS, CURRENT_WATTS,
                INSTALLATION_TIMESTAMP, MEAN_EXPIRATION_TIME, STATE));
    }
}