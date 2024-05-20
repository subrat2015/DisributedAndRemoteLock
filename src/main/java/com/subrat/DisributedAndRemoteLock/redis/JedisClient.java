package com.subrat.DisributedAndRemoteLock.redis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.time.Duration;
import java.util.Objects;

public class JedisClient {
    private final JedisPool jedisPool;

    public JedisClient(int port) {
        this.jedisPool = new JedisPool(buildPoolConfig(), "localhost", port);
    }
    private JedisPoolConfig buildPoolConfig() {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        return poolConfig;
    }

    /*
        We are using threadId as value
        so that we can make sure that the thread which acquired the lock
        is the same thread which released the lock
     */

    public boolean acquireLock(String key,  int ttlInSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            SetParams setParams = new SetParams();
            setParams.nx().ex(ttlInSeconds);
            String threadId = String.valueOf(Thread.currentThread().getId());
            String result = jedis.set(key, threadId, setParams);
            return Objects.nonNull(result);
        }
    }

    public boolean releaseLock(String key) {
        String threadId = String.valueOf(Thread.currentThread().getId());
        String luaScript =
                "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                        "   return redis.call('del', KEYS[1]) " +
                        "else " +
                        "   return 0 " +
                        "end";
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = (Long) jedis.eval(luaScript, 1, key, threadId);
            return result == 1;
        }
    }

}