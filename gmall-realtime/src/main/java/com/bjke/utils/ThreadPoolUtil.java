package com.bjke.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 封装线程池工具类
 */
public class ThreadPoolUtil {

    //声明线程池
    static ThreadPoolExecutor threadPoolExecutor = null;

    public ThreadPoolUtil() {
    }

    public static ThreadPoolExecutor getThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    /*
                      获取单例的线程池对象
                      corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程 去执行，还是放到 workQueue 任务队列中去;
                      maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的 workQueue 任务队列的类型，决定线程池会开辟的最大线程数量;
                      keepAliveTime:当线程池中空闲线程数量超过 corePoolSize 时，多余的线程会在多长时间 内被销毁;
                      unit:keepAliveTime 的单位 workQueue:任务队列，被添加到线程池中，但尚未被执行的任务
                     */
                    threadPoolExecutor = new ThreadPoolExecutor(4, 8, 1L, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
