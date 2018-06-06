package it.uniroma2.sdcc.trafficcontrol.bolts;

import it.uniroma2.sdcc.trafficcontrol.utils.EhCacheManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;

import java.util.Map;

public abstract class AbstractAuthenticationBolt extends BaseRichBolt {

    protected OutputCollector collector;
    private final String cacheName;
    protected EhCacheManager cacheManager;

    public AbstractAuthenticationBolt(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public final void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.cacheManager = new EhCacheManager(cacheName);
    }

}
