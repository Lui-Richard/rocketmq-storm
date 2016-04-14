package com.alibaba.rocketmq.storm.spout.factory;

import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.IRichSpout;

import com.alibaba.rocketmq.storm.annotation.Extension;
import com.alibaba.rocketmq.storm.domain.RocketMQSpouts;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * @author Von Gosling
 */
public final class RocketMQSpoutFactory {
    private static final Logger              logger         = LoggerFactory
                                                                    .getLogger(RocketMQSpoutFactory.class);

    private static Cache<String, IRichSpout> cache          = CacheBuilder.newBuilder().build();

    private static final String              DEFAULT_BROKER = RocketMQSpouts.STREAM.getValue();

    //get spout by key
    public static IRichSpout getSpout(String spoutName,String key) {
        RocketMQSpouts spoutType = RocketMQSpouts.fromString(spoutName);
        switch (spoutType) {
            case SIMPLE:
            case BATCH:
            case STREAM:
                return locateSpout(spoutName,key);
            default:
                logger.warn("Can not support this spout type {} temporarily !", spoutName);
                return locateSpout(DEFAULT_BROKER,"defaultKey");

        }
    }

    private static IRichSpout locateSpout(String spoutName,String key) {
        IRichSpout  spout;

        spout = cache.getIfPresent(key);
        if (null == spout) {
            for (IRichSpout spoutInstance : ServiceLoader.load(IRichSpout.class)) {
                Extension ext = spoutInstance.getClass().getAnnotation(Extension.class);
                if (spoutName.equals(ext.value())) {
                    spout = spoutInstance;
                    cache.put(key, spoutInstance);
                    return spoutInstance;
                }
            }
        }

        return spout;
    }

}
