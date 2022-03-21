/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.media.vnptpay.cache.process;

import com.media.vnptpay.utils.Params;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author MSI
 */
public class PromotionRedisCache {
    static final Logger logger = LogManager.getLogger(PromotionRedisCache.class);
    
    public static String TET_CASHBACK_INFO = "tetCashbackInfo";
    public static String TET_REQUEST_TRANSFER = "tetRequestTransfer";
    
    private static PromotionRedisCache promotionRedisCache = null;
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;
    private RedisCommands<String, String> redisCommand;
    
    public PromotionRedisCache() {
        logger.info("init PromotionRedisCache object!");
        connectToServer();
    }
    
    public static PromotionRedisCache getInstance() {
        if(promotionRedisCache == null) {
            synchronized (PromotionRedisCache .class) {
                if (promotionRedisCache == null) {
                    promotionRedisCache = new PromotionRedisCache();
                }
            }
        }
        return promotionRedisCache;
    }
    
    public void connectToServer() {
        logger.info("connecting to server address: {}:{}", Params.redis_server_address, Params.redis_server_port);
        try {
            RedisURI redisUri = RedisURI.builder().withHost(Params.redis_server_address)
                    .withPort(Params.redis_server_port)
                    .withTimeout(Duration.of(20, ChronoUnit.SECONDS))
                    .build();
            redisClient = RedisClient.create(redisUri);
            logger.info("create redis client with info: {}", redisClient.toString());
            redisConnection = redisClient.connect();
            redisCommand = redisConnection.sync();
            logger.info("Connect to Redis server is successfully!");
        }
        catch (Exception ex) {
            logger.warn("Occurs exception: {}", ex.toString());
        }
    }
    
    public void closeConnectionServer() {
        try {
            redisConnection.close();
            redisClient.shutdown();
        }
        catch (Exception ex) {
            logger.warn("Occurs exception: {}", ex.toString());
        }
    }
    
    public void shutdownRedisClient() {
        try {
            redisClient.shutdown();
        }
        catch (Exception ex) {
            logger.warn("Occurs exception: {}", ex.toString());
        }
    }
    
    public String hGet(String keyRecord, String keyMap){
        if(redisConnection == null) {
            shutdownRedisClient();
            connectToServer();
        }
        else if(!redisConnection.isOpen()) {
            logger.warn("Not connected to RedisServer");
            closeConnectionServer();
            connectToServer();
        }
        String result = redisCommand.hget(keyRecord, keyMap);
        return result;
    }
    
    public boolean hSet(String keyRecord, String keyMap, String valueMap){
        boolean result = false;
        if(redisConnection == null) {
            shutdownRedisClient();
            connectToServer();
        }
        else if(!redisConnection.isOpen()) {
            logger.warn("Not connected to RedisServer");
            closeConnectionServer();
            connectToServer();
        }
        result = redisCommand.hset(keyRecord, keyMap, valueMap);
        return result;
    }
    
    public boolean hExist(String keyRecord, String keyMap){
        boolean result = false;
        if(redisConnection == null) {
            shutdownRedisClient();
            connectToServer();
        }
        else if(!redisConnection.isOpen()) {
            logger.warn("Not connected to RedisServer");
            closeConnectionServer();
            connectToServer();
        }
        result = redisCommand.hexists(keyRecord, keyMap);
        return result;
    }
    
    public long del(String keyRecord){
        if(redisConnection == null) {
            shutdownRedisClient();
            connectToServer();
        }
        else if(!redisConnection.isOpen()){
            logger.warn("Not connected to RedisServer");
            closeConnectionServer();
            connectToServer();
        }
        long count = redisCommand.del(keyRecord);
        return count;
    }
}
