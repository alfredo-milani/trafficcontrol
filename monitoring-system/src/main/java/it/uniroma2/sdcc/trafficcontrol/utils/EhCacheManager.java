package it.uniroma2.sdcc.trafficcontrol.utils;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import java.io.Serializable;

public class EhCacheManager {

    // Non ci può essere più di una istanza di CacheManager per JVM
    private final CacheManager cacheManager;
    private final Cache cache;

    public EhCacheManager(String cacheName) {
        cacheManager = CacheManager.getInstance();
        cache = cacheManager.getCache(cacheName);

        if (cache == null) {
            throw new CacheException(String.format("Cache '%s' non trovata.", cacheName));
        }
    }

    public void put(Serializable key, Serializable value) {
        cache.put(new Element(key, value));
    }

    public Object get(Serializable key) {
        return cache.get(key).getObjectValue();
    }

    public boolean isKeyInCache(Serializable key) {
        return cache.isKeyInCache(key);
    }

    public int size() {
        return cache.getSize();
    }

    public void shutdownCacheManager() {
        cacheManager.shutdown();
    }

    public void addCache(String cacheName) {
        cacheManager.addCache(cacheName);
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

}
