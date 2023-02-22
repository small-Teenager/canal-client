package com.example.canalclient.config;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;
import java.util.Arrays;

@Component
public class CanalConfig {

    protected final static Logger logger = LoggerFactory.getLogger(CanalConfig.class);
    @Value("${canal.host}")
    private String host;

    @Value("${canal.port}")
    private Integer port;

    @Value("${canal.destination}")
    private String destination;

    @Value("${canal.username}")
    private String username;

    @Value("${canal.password}")
    private String password;

    @Value("${canal.subscribe}")
    private String subscribe;

    @Bean
    public CanalConnector getCanalConnector() {
        CanalConnector canalConnector = CanalConnectors.newClusterConnector(Arrays.asList(new InetSocketAddress(host, port)), destination, username, password);
        canalConnector.connect();
        // 指定要订阅的数据库和表
        canalConnector.subscribe(subscribe);
        // 回滚到上次中断的位置
        canalConnector.rollback();

        logger.info("canal客户端启动......");
        return canalConnector;
    }
}
