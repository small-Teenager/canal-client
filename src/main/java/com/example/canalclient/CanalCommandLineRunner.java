package com.example.canalclient;

import com.alibaba.otter.canal.client.CanalConnector;
import com.example.canalclient.config.CanalConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * @author: yaodong zhang
 * @create 2023/2/8
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CanalCommandLineRunner implements CommandLineRunner {
    protected final static Logger logger = LoggerFactory.getLogger(CanalConfig.class);

    @Value("${canal.destination}")
    private String destination;

    @Value("${canal.batchSize}")
    private int batchSize;

    @Autowired
    private CanalConnector canalConnector;

    @Override
    public void run(String... args) throws Exception {

        SimpleCanalClient simpleCanalClient = new SimpleCanalClient(destination, batchSize);
        simpleCanalClient.setConnector(canalConnector);
        simpleCanalClient.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                simpleCanalClient.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## canal client is down.");
            }
        }));
    }
}
