package it.uniroma2.sdcc.trafficcontrol;

import it.uniroma2.sdcc.trafficcontrol.topology.SecondTopology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import static it.uniroma2.sdcc.trafficcontrol.constants.KafkaParams.KAFKA_IP_PORT;


public class Starter {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config conf = new Config();
        LocalCluster localCluster = new LocalCluster();
        try {
            KAFKA_IP_PORT = args[0].concat(":9092");
            int stormWorkers = Integer.parseInt(args[1]);
            //conf.setNumWorkers(stormWorkers);
        } catch (Exception e) {
            System.err.println("You have to specify kafka host and the number of the workers");
            e.printStackTrace();
        }

        TopologyBuilder builder = null;
        //builder = FirstTopology.setTopology(builder);
        builder = SecondTopology.setTopology(builder);
        //builder = ThirdTopology.setTopology(builder);
        //builder = FourthTopology.setTopology(builder);

        localCluster.submitTopology("s",conf,builder.createTopology());
        //StormSubmitter.submitTopology("monitoring-system", conf, builder.createTopology());

        /*
        FirstTopology firstTopology = new FirstTopology();
        SecondTopology secondTopology = new SecondTopology();
        ThirdTopology thirdTopology = new ThirdTopology();
        FourthTopology fourthTopology = new FourthTopology();


        //cluster.submitTopology("first_topology", conf, firstTopology.createTopology());
        //cluster.submitTopology("second_topology", conf, secondTopology.createTopology());
        //cluster.submitTopology("third_topology", conf, thirdTopology.createTopology());
        //cluster.submitTopology("fourth_topology", conf, fourthTopology.createTopology());
        StormSubmitter.submitTopology("1",conf,thirdTopology.createTopology());
        */
    }
}
