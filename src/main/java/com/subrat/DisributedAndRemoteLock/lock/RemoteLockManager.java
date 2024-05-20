package com.subrat.DisributedAndRemoteLock.lock;

import com.subrat.DisributedAndRemoteLock.redis.JedisClient;

public class RemoteLockManager {
    private final JedisClient jedisClient;


    public RemoteLockManager(JedisClient jedisClient) {
        this.jedisClient = jedisClient;
    }

    public void processTask(String key) {

        while (true)  {
            try {
                System.out.println("Current Thread : " + Thread.currentThread().getId() + " acquiring lock ");
                boolean lockAcquired = jedisClient.acquireLock(key, 300);
                if (lockAcquired) {
                    System.out.println("Current Thread : " + Thread.currentThread().getId() + " successfully acquired lock ");
                    System.out.println("processing  task");
                    Thread.sleep(1000);
                    System.out.println("processing  task completed");
                    return;
                }
            } catch (Exception e) {
                System.out.println("Error  : " + e.getMessage());
                jedisClient.releaseLock(key);
            }
            finally {
                if(jedisClient.releaseLock(key)); {
                    System.out.println("Current Thread : " + Thread.currentThread().getId() + " releasing lock successfully");
                }
            }
        }
    }

}