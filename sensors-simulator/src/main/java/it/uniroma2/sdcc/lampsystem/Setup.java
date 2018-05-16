package it.uniroma2.sdcc.lampsystem;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;


public class Setup {
    public static final Integer MAX_CELLS_PER_LAMPS = 4;
    public static final Integer MAX_STOPS_PER_LAMPS = 4;

    public static void main(String[] args) throws IOException {

        Integer lampID = 0;
        Random random = new Random(System.currentTimeMillis());
        int maxW = 300;
        int minW = 60;

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(new File("input.json"));

        ArrayNode cities = (ArrayNode) jsonNode.get("cities");
        for (int i = 0; i < cities.size(); i++) {
            String city = cities.get(i).get("name").asText();
            ArrayNode parks = (ArrayNode) cities.get(i).get("parks");
            ArrayNode busStops = (ArrayNode) cities.get(i).get("bus_stops");
/*
            for (int j = 0; j < parks.size(); j++) {
                Integer parkID = parks.get(j).get("id").asInt();
                String address = parks.get(j).get("address").asText();
                ArrayNode cells = (ArrayNode) parks.get(j).get("cellsID");
                for (int k = 0; k < cells.size(); k++) {
                    Integer bound;

                    //System.out.println(cells.size() + " " + k);
                    if (k + MAX_CELLS_PER_LAMPS < cells.size()) {
                        bound = random.nextInt(4) + 1;
                    } else {
                        bound = random.nextInt(cells.size() - k ) + 1;
                    }

                    ArrayList<Integer> cellsIDs = new ArrayList<Integer>();
                    for (int l = k; l < k + bound; l++) {
                        cellsIDs.add(cells.get(l).asInt());
                    }
                    int bulbMaxWatt = random.nextInt((maxW - minW) + 1) + minW;
                    LightBulb lightBulb = new LightBulb("E27", bulbMaxWatt, System.currentTimeMillis(), (long)14400000 * 1000);
                    new Sensor(lampID++,city,address,4,"park",parkID,cellsIDs,lightBulb);
                    //TODO FARE LAMPIONE
                }
            }
            */
            for (int j = 0; j < busStops.size(); j++) {
                Integer bus_stops = busStops.get(j).get("id").asInt();
                String address = busStops.get(j).get("address").asText();
                ArrayNode cells = (ArrayNode) busStops.get(j).get("cellsID");
                for (int k = 0; k < cells.size(); k++) {
                    Integer bound;

                    if (k + MAX_STOPS_PER_LAMPS < cells.size()) {
                        bound = random.nextInt(4) + 1;
                    } else {
                        bound = random.nextInt(cells.size() - k ) + 1;
                    }

                    ArrayList<Integer> cellsIDs = new ArrayList<Integer>();
                    for (int l = k; l < k + bound; l++) {
                        cellsIDs.add(l);
                    }

                    int bulbMaxWatt = random.nextInt((maxW - minW) + 1) + minW;
                    LightBulb lightBulb = new LightBulb("E27", bulbMaxWatt, System.currentTimeMillis(), (long)14400000 * 1000);
                    new Sensor(lampID++,city,address,4,"bus_stop",bus_stops,cellsIDs,lightBulb);
                }
            }
        }

        //TODO sistemare costanti


        /*
        int maxW = 300;
        int minW = 60;

        Random rand = new Random(System.currentTimeMillis());

        InputStream fis = new FileInputStream("input.txt");
        InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(isr);
        String line = "";
        int j = 0;
//while ((line = br.readLine()) != null) {
        for (j = 0; j < 10; j++) {
            line = br.readLine();
            String city = "";
            String street = "";
            String number = "";
            String ID = "";
            int bulbMaxWatt = rand.nextInt((maxW - minW) + 1) + minW;
            LightBulb lightBulb = new LightBulb("E27", bulbMaxWatt, System.currentTimeMillis(), (long)60 * 1000);
            int pos = 0;
            //parse ID
            while (line.charAt(pos) != ';') {
                ID += line.charAt(pos);
                pos++;
            }
            pos++;
            //parse city
            while(line.charAt(pos) != ';') {
                city += line.charAt(pos);
                pos ++;
            }
            pos ++;
            //parse street
            while (line.charAt(pos) != ';') {
                street += line.charAt(pos);
                pos++;
            }
            pos++;
            //parse number
            while (pos < line.length()) {
                number += line.charAt(pos);
                pos++;
            }
            new Sensor(Integer.parseInt(ID), city, street, Integer.parseInt(number), lightBulb);
        }
    */
    }
}
