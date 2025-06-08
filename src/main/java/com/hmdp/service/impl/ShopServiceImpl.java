package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.BooleanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }

        // 返回
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id) {
        // 从 redis 中查询
        String shopKey = CACHE_SHOP_KEY + id;
        Map<Object, Object> shopMap = stringRedisTemplate.opsForHash().entries(shopKey);
        // 存在，直接返回
        if (!shopMap.isEmpty()) {
            return null;
        }

        // 命中，判断过期时间
        RedisData redisData = new RedisData();
        BeanUtil.fillBeanWithMap(shopMap, redisData, CopyOptions.create().setIgnoreNullValue(true));
        Shop shop = (Shop) redisData.getData();
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 未过期，直接返回
            return shop;
        }

        // 已过期，缓存重建
        // 获取互斥锁
        boolean isLock = tryLock(id.toString());
        // 判断是否获取成功
        if (!isLock) {
            // 成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    saveShopToRedis(id, 1800L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unlock(id.toString());
                }
            });
        }

        // 返回过期的店铺信息
        return shop;
    }

    public Shop queryWithMutex(Long id) {
        // 从 redis 中查询
        String shopKey = CACHE_SHOP_KEY + id;
        Map<Object, Object> shopMap = stringRedisTemplate.opsForHash().entries(shopKey);
        Shop shop = new Shop();
        // 命中空对象
        if (shopMap.get("") != null) {
            return null;
        }
        // 存在，直接返回
        if (!shopMap.isEmpty()) {
            BeanUtil.fillBeanWithMap(shopMap, shop, CopyOptions.create().setIgnoreNullValue(true));
            return shop;
        }

        // 实现缓存重建
        // 获取互斥锁
        try {
            boolean isLock = tryLock(id.toString());
            // 判断是否获取成功
            if (!isLock) {
                // 失败，休眠并重试
                Thread.sleep(50);
                queryWithMutex(id);
            }

            // 成功，查询数据库
            shop = getById(id);
            // 不存在，保存空对象，返回错误
            if (shop == null) {
                stringRedisTemplate.opsForHash().put(shopKey, "","");
                stringRedisTemplate.expire(shopKey, CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 存在，存入 redis
            Map<String, Object> cachedShopMap = BeanUtil.beanToMap(shop, new HashMap<>(), CopyOptions.create()
                    .setIgnoreNullValue(true)
                    .setFieldValueEditor((field, fieldValue) -> fieldValue == null ? null : fieldValue.toString())
            );
            stringRedisTemplate.opsForHash().putAll(shopKey, cachedShopMap);
            stringRedisTemplate.expire(shopKey, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放互斥锁
            unlock(id.toString());
        }

        // 返回
        return shop;
    }

    public Shop queryWithPassThrough(Long id) {
        // 从 redis 中查询
        String shopKey = CACHE_SHOP_KEY + id;
        Map<Object, Object> shopMap = stringRedisTemplate.opsForHash().entries(shopKey);
        Shop shop = new Shop();
        // 命中空对象
        if (shopMap.get("") != null) {
            return null;
        }
        // 存在，直接返回
        if (!shopMap.isEmpty()) {
            BeanUtil.fillBeanWithMap(shopMap, shop, CopyOptions.create().setIgnoreNullValue(true));
            return shop;
        }

        // 不存在，查询数据库
        shop = getById(id);
        // 不存在，保存空对象，返回错误
        if (shop == null) {
            stringRedisTemplate.opsForHash().put(shopKey, "","");
            stringRedisTemplate.expire(shopKey, CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 存在，存入 redis
        Map<String, Object> cachedShopMap = BeanUtil.beanToMap(shop, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((field, fieldValue) -> fieldValue == null ? null : fieldValue.toString())
        );
        stringRedisTemplate.opsForHash().putAll(shopKey, cachedShopMap);
        stringRedisTemplate.expire(shopKey, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        // 返回
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(LOCK_SHOP_KEY + key, "1",
                LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(LOCK_SHOP_KEY + key);
    }

    private void saveShopToRedis(Long id, Long expireSeconds) {
        // 查询店铺
        Shop shop = getById(id);
        // 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 写入 redis
        String shopKey = CACHE_SHOP_KEY + id;
        Map<String, Object> redisDataMap = BeanUtil.beanToMap(redisData, new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((field, fieldValue) -> fieldValue == null ? null : fieldValue.toString())
        );
        stringRedisTemplate.opsForHash().putAll(shopKey, redisDataMap);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺 id 不能为空");
        }
        // 更新数据库
        updateById(shop);
        // 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return null;
    }
}
