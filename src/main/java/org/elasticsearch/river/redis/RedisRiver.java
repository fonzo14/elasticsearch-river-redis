package org.elasticsearch.river.redis;

import java.util.List;
import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisRiver extends AbstractRiverComponent implements River {
	
	private final Client client;
	
	private final String  redisHost;
	private final int     redisPort;
	private final String  redisKey;
	private final int     redisDB;
	
	private final int bulkSize;
    private final TimeValue bulkTimeout;

    private volatile boolean closed = false;

    private volatile Thread thread;
    
    private volatile JedisPool jedisPool;
	
	@SuppressWarnings({"unchecked"})
    @Inject
    public RedisRiver(RiverName riverName, RiverSettings settings, Client client) {
		super(riverName, settings);
		this.client = client;
		
		/* Build up the settings */  
		if(settings.settings().containsKey("redis")) {
			Map<String, Object> redisSettings = (Map<String, Object>) settings.settings().get("redis");
			redisHost = XContentMapValues.nodeStringValue(redisSettings.get("host"), "localhost");
			redisPort = XContentMapValues.nodeIntegerValue(redisSettings.get("port"), 6379);
			redisKey  = XContentMapValues.nodeStringValue(redisSettings.get("key"), "redis_river");
			redisDB   = XContentMapValues.nodeIntegerValue(redisSettings.get("database"), 0);
		} else {
			redisHost = "localhost";
			redisPort = 6379;
			redisKey  = "elasticsearch:redis:river";
			redisDB   = 0;
		}
		
		if (settings.settings().containsKey("index")) {
            Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
            bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
            if (indexSettings.containsKey("bulk_timeout")) {
                bulkTimeout = TimeValue.parseTimeValue(XContentMapValues.nodeStringValue(indexSettings.get("bulk_timeout"), "10s"), TimeValue.timeValueSeconds(10));
            } else {
                bulkTimeout = TimeValue.timeValueSeconds(10);
            }
        } else {
            bulkSize = 100;
            bulkTimeout = TimeValue.timeValueSeconds(10);
        }
	}

	@Override
	public void start() {
		try {
			this.jedisPool = new JedisPool(this.redisHost, this.redisPort);  
		} catch (Exception e) {
			logger.error("Unable to allocate redis pool. Disabling River.");
			return;
		}
		
		logger.info("creating redis river, host [{}], port [{}], db [{}], key [{}]", redisHost, redisPort, redisDB, redisKey);

		thread = EsExecutors.daemonThreadFactory(settings.globalSettings(), "redis_river").newThread(new Poper());
		thread.start();
	}
	
	@Override
    public void close() {
        if (closed) {
            return;
        }
        
        logger.info("closing redis river");
        
        closed = true;
        
        if (thread != null) {
        	thread.interrupt();
        }
    }
	
	private class Poper implements Runnable {

		private Jedis jedis;
		
		@Override
		public void run() {
			while (true) {
                if (closed) {
                    break;
                }
                
                try {
    				jedis = jedisPool.getResource();
    				if (redisDB > 0) {
    					jedis.select(redisDB);
    				}
                } catch (Exception e) {
                    if (!closed) {
                        logger.warn("failed to create a connection to redis", e);
                    } else {
                        continue;
                    }
                    cleanup(0, "failed to connect");
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e1) {
                        // ignore, if we are closing, we will exit later
                    }
                }
                
                // now use the connection to pop messages
                while (true) {
                    if (closed) {
                        break;
                    }
                    List<String> response = null;
                    try {
                    	response = jedis.blpop((int)bulkTimeout.getSeconds(), redisKey);
                    } catch (Exception e) {
                        if (!closed) {
                            logger.error("failed to pop message, reconnecting...", e);
                        }
                        cleanup(0, "failed to get message");
                        break;
                    }
                    
                    if (response != null) {
                    	
                    	BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
                    	
                    	try {
                    		byte[] data = response.get(1).getBytes();
                            bulkRequestBuilder.add(data, 0, data.length, false);
                        } catch (Exception e) {
                            logger.warn("failed to add item to bulk [{}]", e);
                            continue;
                        }
                        
                        try {
                        	List<String> items = jedis.lrange(redisKey, 0, bulkSize - 2);
                        	if (items != null) {
                        		for (String item : items) {
                        			try {
                        				byte[] data = item.getBytes();
                        				bulkRequestBuilder.add(data, 0, data.length, false);
                        			} catch (Exception e) {
                        				logger.warn("failed to add item to bulk [{}]", e);
                        			}
                        		}
                        	}
                        } catch (Exception e) {
                        	if (closed) {
                        		break;
                        	}
                        }
                        
                        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                if (response.hasFailures()) {
                                    // TODO write to exception queue?
                                    logger.warn("failed to execute" + response.buildFailureMessage());
                                }
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                logger.warn("failed to execute bulk [{}]", e);
                            }
                        });
                    } else {
                    	if (logger.isDebugEnabled()) logger.debug("Nothing popped. Timed out.");
                    }
                    
                }
                
			}
			cleanup(0, "closing redis river");
		}
		
		private void cleanup(int code, String message) {
            try {
            	if (jedis != null) {
            		jedisPool.returnBrokenResource(jedis);
            	}
            } catch (Exception e) {
                logger.debug("failed to close broken resource on [{}]", e, message);
            }
        }
		
	}

	
}
