package com.example.canatest.config;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.huazhu.basejarservice.canal.client.interfaces.CanalEventListener;
import org.springframework.stereotype.Component;

/**
 * @author lee
 * @date 2019/2/28
 */
@Component
public class MyEventListener2 implements CanalEventListener {
    /*@Override
    public void onEvent(CanalEntry.EventType eventType, CanalEntry.RowData rowData) {
        //rowData.getAfterColumnsList().forEach((c) -> System.err.println("By--implements :" + c.getName() + " ::   " + c.getValue()));
    }*/

    @Override
    public void onEvent(String s, String s1, String s2, CanalEntry.RowChange rowChange) {

    }
}
