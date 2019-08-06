package org.micro.tcc.common.core;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class FixSizeCacheMap {

    private static final Cache<String, Object> cache = CacheBuilder.newBuilder().maximumSize(1000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    private static FixSizeCacheMap fixSizeCacheMap =new FixSizeCacheMap();

    public static FixSizeCacheMap get(){
        return fixSizeCacheMap;
    }

    public  void add(String key,Object value){
        cache.put(key,value);
    }

    public Object peek(String key){
        return cache.getIfPresent(key);
    }

    public void del(String key){
        cache.invalidate(key);
    }

}
