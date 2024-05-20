package com.subrat.DisributedAndRemoteLock.lock;

import com.subrat.DisributedAndRemoteLock.redis.JedisClient;

import java.util.List;
import java.util.Random;

public class DistributedLockManager {

    private final List<JedisClient> jedisClients;
    private final Random random;

    public DistributedLockManager(List<JedisClient> jedisClients) {
        this.jedisClients = jedisClients;
        this.random = new Random();
    }

    public void processTask(String key) {
        while (true)  {
            try {
                System.out.println("Current Thread : " + Thread.currentThread().getId() + " acquiring lock ");
                boolean lockAcquired = acquireLock(key);
                if (lockAcquired) {
                    System.out.println("Current Thread : " + Thread.currentThread().getId() + " successfully acquired lock ");
                    System.out.println("processing  task");
                    Thread.sleep(1000);
                    System.out.println("processing  task completed");
                    return;
                }
            } catch (Exception e) {
                System.out.println("Error  : " + e.getMessage());
                releaseLock(key);
            }
            finally {
                releaseLock(key);
            }
        }
    }


    private boolean acquireLock(String key) {
        int count = 0;
        for (int i = 0; i<jedisClients.size(); i++) {
            int index = getRandomIndex();
            if(jedisClients.get(index).acquireLock(key, 300)) {
                count++;
                System.out.println("Current Thread : " + Thread.currentThread().getName() + " lock acquired " + index);
            }
        }

        if (count > jedisClients.size()/2) {
            return true;
        } else {
            for (JedisClient jedisClient : jedisClients) {
                jedisClient.releaseLock(key);
            }
        }
        return false;
    }

    private void releaseLock(String key) {
        for (JedisClient jedisClient : jedisClients) {
            if (jedisClient.releaseLock(key)) {
                System.out.println("Current Thread : " + Thread.currentThread().getId() + " releasing lock successfully");
            }
        }
    }

    private int getRandomIndex() {
        return random.nextInt(jedisClients.size());
    }

}
