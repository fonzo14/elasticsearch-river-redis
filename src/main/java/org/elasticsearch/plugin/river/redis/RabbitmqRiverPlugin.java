package org.elasticsearch.plugin.river.redis;


import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.redis.RedisRiverModule;

public class RabbitmqRiverPlugin extends AbstractPlugin {

    @Inject
    public RabbitmqRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-redis";
    }

    @Override
    public String description() {
        return "River Redis Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("redis", RedisRiverModule.class);
    }
}