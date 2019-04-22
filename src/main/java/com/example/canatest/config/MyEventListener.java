package com.example.canatest.config;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.example.canatest.util.RedisUtil;
import com.huazhu.basejarservice.canal.annotation.*;
import com.huazhu.basejarservice.canal.annotation.content.UpdateListenPoint;
import com.huazhu.basejarservice.canal.client.core.CanalMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.util.Calendar;
import java.util.List;

/**
 * @author chen.qian
 * @date 2018/3/19
 */
@CanalEventListener
public class MyEventListener {
    public static final Logger log = LoggerFactory.getLogger(MyEventListener.class);

    /*@InsertListenPoint(destination = "example", schema = "device", table = {"t_device_copy"})
    public void onEvent(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        rowData.getAfterColumnsList().forEach((c) -> System.err.println("By--Annotation: " + c.getName() + " ::   " + c.getValue()));
    }

    @UpdateListenPoint(destination = "example", schema = "device", table = {"t_device_copy"})
    public void onEvent1(CanalEntry.RowData rowData) {
        System.err.println("UpdateListenPoint");
        rowData.getAfterColumnsList().forEach((c) -> System.err.println("By--Annotation: " + c.getName() + " ::   " + c.getValue()));
    }

    @DeleteListenPoint(destination = "example", schema = "device", table = {"t_device_copy"})
    public void onEvent3(CanalEntry.EventType eventType) {
        System.err.println("DeleteListenPoint");
    }*/

    /*@UpdateListenPoint(destination = "example", schema = "device", table = {"t_device", "t_alarm"})
    public void onEventUpdateData(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        System.out.println("======================注解方式（更新数据操作）==========================");
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {

            String sql = "use " + canalMsg.getSchemaName() + ";\n";
            StringBuffer updates = new StringBuffer();
            StringBuffer conditions = new StringBuffer();
            rowData.getAfterColumnsList().forEach((c) -> {
                if (c.getIsKey()) {
                    conditions.append(c.getName() + "='" + c.getValue() + "'");
                } else {
                    updates.append(c.getName() + "='" + c.getValue() + "',");
                }
            });
            sql += "UPDATE " + canalMsg.getTableName() + " SET " + updates.substring(0, updates.length() - 1) + " WHERE " + conditions;
            System.out.println(sql);
        }
        System.out.println("\n======================================================");
    }*/
   @ListenPoint(destination = "example", schema = "canal", table = {"address", "info"},eventType = CanalEntry.EventType.UPDATE)
    public void onEvent1(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
       log.info("======================更新数据操作==========================");
       String tableName = canalMsg.getTableName();
       List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
       for (CanalEntry.RowData rowData : rowDatasList) {
           String sql = "use " + canalMsg.getSchemaName() + ";\n";
           StringBuffer updates = new StringBuffer();
           StringBuffer conditions = new StringBuffer();
           List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
           redisUpdate(afterColumnsList, tableName);
           afterColumnsList.forEach((c) -> {
               if (c.getIsKey()) {
                   conditions.append(c.getName() + "='" + c.getValue() + "'");
               } else {
                   updates.append(c.getName() + "='" + c.getValue() + "',");
               }
           });
           sql += "UPDATE " + tableName + " SET " + updates.substring(0, updates.length() - 1) + " WHERE " + conditions;
           log.info(sql);
       }

    }

    @ListenPoint(destination = "example", schema = "canal", table = {"address", "info"},eventType = CanalEntry.EventType.INSERT)
    public void onEvent2(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("======================新增数据操作==========================");
        String tableName = canalMsg.getTableName();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            String sql = "use " + canalMsg.getSchemaName() + ";\n";
            StringBuffer colums = new StringBuffer();
            StringBuffer values = new StringBuffer();
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            redisInsert(afterColumnsList, canalMsg.getTableName());
            afterColumnsList.forEach((c) -> {
                colums.append(c.getName() + ",");
                values.append("'" + c.getValue() + "',");
            });
            sql += "INSERT INTO " + tableName + "(" + colums.substring(0, colums.length() - 1) + ") VALUES(" + values.substring(0, values.length() - 1) + ");";
            log.info(sql);
        }
    }

    @ListenPoint(destination = "example", schema = "canal", table = {"address", "info"},eventType = CanalEntry.EventType.DELETE)
    public void onEvent3(CanalMsg canalMsg, CanalEntry.RowChange rowChange) {
        log.info("======================删除数据操作==========================");
        String tableName = canalMsg.getTableName();
        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
        for (CanalEntry.RowData rowData : rowDatasList) {
            if (!CollectionUtils.isEmpty(rowData.getBeforeColumnsList())) {
                String sql = "use " + canalMsg.getSchemaName() + ";\n";
                sql += "DELETE FROM " + tableName + " WHERE ";
                StringBuffer idKey = new StringBuffer();
                StringBuffer idValue = new StringBuffer();
                redisDelete(rowData.getBeforeColumnsList(), tableName);
                for (CanalEntry.Column c : rowData.getBeforeColumnsList()) {
                    if (c.getIsKey()) {
                        idKey.append(c.getName());
                        idValue.append(c.getValue());
                        break;
                    }
                }

                sql += idKey + " =" + idValue + ";";

                log.info(sql);
            }
        }
    }


    private static void redisInsert(List<CanalEntry.Column> columns, String tableName){
        JSONObject json=new JSONObject();
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        if(columns.size()>0){
            RedisUtil.stringSet(tableName + ":"+ columns.get(0).getValue(),json.toJSONString());
        }
    }

    private static  void redisUpdate(List<CanalEntry.Column> columns, String tableName){
        JSONObject json=new JSONObject();
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        if(columns.size()>0){
            RedisUtil.stringSet(tableName + ":"+ columns.get(0).getValue(),json.toJSONString());
        }
    }

    private static  void redisDelete(List<CanalEntry.Column> columns, String tableName){
        JSONObject json=new JSONObject();
        for (CanalEntry.Column column : columns) {
            json.put(column.getName(), column.getValue());
        }
        if(columns.size()>0){
            RedisUtil.delKey(tableName + ":"+ columns.get(0).getValue());
        }
    }
}
