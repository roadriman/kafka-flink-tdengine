package com.rrm.bean;

import java.util.List;
import java.util.Map;

public class TdengineData {
    private Map<String,String> tags;
    private Long ts;
    private List<TdengineTagData> tagDataList;
    private String topicName;
    private int  partition;
    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public List<TdengineTagData> getTagDataList() {
        return tagDataList;
    }

    public void setTagDataList(List<TdengineTagData> tagDataList) {
        this.tagDataList = tagDataList;
    }

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }
}
