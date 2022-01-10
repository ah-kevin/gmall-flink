package com.bjke.app.function;


import com.alibaba.fastjson.JSONObject;
import com.bjke.common.GmallConfig;
import com.bjke.utils.DimUtil;
import com.bjke.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    private Connection connection;
    private ThreadPoolExecutor threadPoolExecutor;
    //声明属性
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        threadPoolExecutor = ThreadPoolUtil.getThreadPool();
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        System.out.println("TimeOut: " + input);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                // 获取查询的主键
                String key = getKey(input);
                try {
                    // 查询维度信息
                    JSONObject dimInfo = DimUtil.getDimInfo(connection, tableName, key);
                    // 补充维度信息
                    if (dimInfo != null) {
                        join(input,dimInfo);
                    }
                    // 将数据输出
                    resultFuture.complete(Collections.singletonList(input));
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

}
