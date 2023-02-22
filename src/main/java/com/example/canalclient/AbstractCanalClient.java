package com.example.canalclient;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.MDC;
import org.springframework.util.Assert;

/**
 * 测试基类
 * 
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCanalClient extends BaseCanalClient {

    public AbstractCanalClient(String destination,int batchSize){
        this(destination,batchSize, null);
    }

    public AbstractCanalClient(String destination,int batchSize, CanalConnector connector){
        this.destination = destination;
        this.batchSize = batchSize;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);

        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
//        int batchSize = 5 * 1024;
        while (running) {
            long batchId = 0;
            try {
                MDC.put("destination", destination);
                while (running) {
                    // 获取指定数量的数据
                    Message message = connector.getWithoutAck(batchSize);
                    batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
//                        printSummary(message, batchId, size);
                        printEntry(message.getEntries());
                    }

                    if (batchId != -1) {
                        // 提交确认
                        connector.ack(batchId);
                    }
                }
            } catch (Throwable e) {
                logger.error("process error!", e);
                logger.error("批量获取 mysql 同步信息失败，batchId回滚,batchId=" + batchId, e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }
                connector.rollback(batchId); // 处理失败, 回滚数据
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

}
