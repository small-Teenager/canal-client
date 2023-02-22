package com.example.canalclient;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BaseCanalClient {

    protected final static Logger logger = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String SEP = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean running = false;
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error",
            e);
    protected Thread thread = null;
    protected CanalConnector connector;
    protected static String context_format = null;
    protected static String row_format = null;
    protected static String transaction_format = null;
    protected String destination;
    protected int batchSize;

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {}({}) , gtid : ({}) , delay : {} ms"
                + SEP;

        transaction_format = SEP
                + "================> binlog[{}:{}] , executeTime : {}({}) , gtid : ({}) , delay : {}ms"
                + SEP;

    }

    /**
     * 打印摘要信息
     *
     * @param message
     * @param batchId
     * @param size
     */
    protected void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[]{batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition});
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        String position = entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
        if (StringUtils.isNotEmpty(entry.getHeader().getGtid())) {
            position += " gtid(" + entry.getHeader().getGtid() + ")";
        }
        return position;
    }

    protected void printEntry(List<Entry> entrys) {
        for (Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;
            Date date = new Date(entry.getHeader().getExecuteTime());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            // 为行数据
            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChange = null;
                try {
                    rowChange = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                EventType eventType = rowChange.getEventType();

                //发生写入操作的库名
                String database = entry.getHeader().getSchemaName();
                //发生写入操作的表名
                String table = entry.getHeader().getTableName();
                logger.info("监听到数据库: {},表: {} 的 {} 事件", database, table, eventType);
                if(rowChange.getIsDdl()){
                    logger.info("sql: {}",rowChange.getSql());
                }

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), simpleDateFormat.format(date),
                                entry.getHeader().getGtid(), String.valueOf(delayTime)});

                // TODO 解析数据并处理
                for (RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType == EventType.DELETE) {
                        printDeletedColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == EventType.INSERT) {
                        printInsertColumn(rowData.getAfterColumnsList());
                    } else {
                        printUpdateColumn(rowData);
                    }
                }
            }
        }
    }
// 验证操作多行数据
    private void printDeletedColumn(List<Column> columns) {
        Map<String, String> map = this.columnsToMap(columns);
        String jsonStr = JSONObject.toJSONString(map);
        logger.info("删除数据：{}\r\n", jsonStr);
    }

    private void printUpdateColumn(RowData rowData) {
        //每一行的每列数据  字段名->值
        List<Column> beforeColumnsList = rowData.getBeforeColumnsList();
        Map<String, String> beforeMap = this.columnsToMap(beforeColumnsList);
        logger.info("更新前数据：{}", JSONObject.toJSONString(beforeMap));

        List<Column> afterColumnsList = rowData.getAfterColumnsList();
        Map<String, String> afterMap = this.columnsToMap(afterColumnsList);
        String afterJsonStr = JSONObject.toJSONString(afterMap);
        logger.info("更新后数据：{}\r\n", afterJsonStr);
        /**
         *  高并发下，为保证数据一致性，当数据库更新后，不建议去更新缓存，
         *  而是建议直接删除缓存，由查询时再设置到缓存。
         */
    }

    private void printInsertColumn(List<Column> columns) {
        Map<String, String> map = this.columnsToMap(columns);
        String jsonStr = JSONObject.toJSONString(map);
        logger.info("新增数据：{}\r\n", jsonStr);
    }

    /**
     * columns 转 map
     */
    protected Map<String, String> columnsToMap(List<Column> columns) {
        return columns.stream().collect(Collectors.toMap(Column::getName, Column::getValue));
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }



}
