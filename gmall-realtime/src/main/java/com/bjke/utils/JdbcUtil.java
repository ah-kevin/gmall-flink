package com.bjke.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    /**
     * select * from t1;
     * xxx,xx,xx
     * xxx,xx,xx
     *
     * @param connection
     * @param querySql
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws SQLException {
        // 创建集合用户存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();
        // 预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        // 执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        // 解析ResultSet
//        while (resultSet) {
//            // 创建泛型对
//            T t = clz.newInstance();
//            // 给泛型复制
//
//            // 将对象添加至集合
//        }
        return resultList;
    }
}
