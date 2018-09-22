package it.uniroma2.sdcc.trafficcontrol.boltsValidation;

import it.uniroma2.sdcc.trafficcontrol.RESTfulAPI.RESTfulAPI;
import it.uniroma2.sdcc.trafficcontrol.abstractsBolts.AbstractAuthenticationBolt;
import it.uniroma2.sdcc.trafficcontrol.entity.configuration.AppConfig;
import it.uniroma2.sdcc.trafficcontrol.entity.sensors.RichSemaphoreSensor;
import it.uniroma2.sdcc.trafficcontrol.exceptions.BadEndpointException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static it.uniroma2.sdcc.trafficcontrol.constants.SemaphoreSensorTuple.SEMAPHORE_SENSOR;

public class SemaphoreAuthByEndpointBolt extends AbstractAuthenticationBolt {

    // File di configurazione
    private final AppConfig appConfig;
    private final RESTfulAPI restfulAPI;
    private final String semaphoreSensorEndpoint;

    public SemaphoreAuthByEndpointBolt(AppConfig appConfig, String cacheName) {
        super(cacheName);

        this.appConfig = appConfig;
        restfulAPI = new RESTfulAPI(appConfig);
        semaphoreSensorEndpoint = appConfig.getSemaphoresSensorsEndpoint();
        if (semaphoreSensorEndpoint == null) {
            throw new BadEndpointException(String.format(
                    "Si deve specificare un endpoint valido. Endpoint corrente: \"%s\"",
                    semaphoreSensorEndpoint
            ));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        RichSemaphoreSensor semaphoreSensor = (RichSemaphoreSensor) tuple.getValueByField(SEMAPHORE_SENSOR);

        boolean semaphoreInSystem;
        synchronized (cacheManager.getCacheManager()) {
            // Double checked lock
            if (!(semaphoreInSystem = cacheManager.isKeyInCache(semaphoreSensor.getSemaphoreId()))) {
                if (semaphoreInSystem = restfulAPI.sensorExistsWithIdFromEndpoint(semaphoreSensor.getSemaphoreId(), semaphoreSensorEndpoint)) {
                    cacheManager.put(semaphoreSensor.getSemaphoreId(), semaphoreSensor.getSemaphoreId());
                }
            }
        }

        if (semaphoreInSystem) {
            collector.emit(new Values(semaphoreSensor));
        }

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SEMAPHORE_SENSOR));
    }

}
