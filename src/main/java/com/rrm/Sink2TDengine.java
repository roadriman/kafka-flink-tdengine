package com.rrm;

import com.rrm.bean.TdengineData;
import com.rrm.bean.TdengineTableData;
import com.rrm.bean.TdengineTagData;
import com.rrm.utils.TDengineStmtUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


public class Sink2TDengine extends RichSinkFunction<TdengineData> {
    final static Logger logger = LoggerFactory.getLogger(Sink2TDengine.class);

    private Connection connection;
    private Statement statement;
    private String dbName;
    private String superTableName;
    private String tableName;
    private int partition;
    private AtomicInteger count = new AtomicInteger();
    private AtomicInteger total = new AtomicInteger();
    private Map<String, CaseInsensitiveMap> tableMap = new HashMap<>();
    private long lastTime = 0;
    private ConcurrentLinkedDeque<String> sqlDeque = new ConcurrentLinkedDeque<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.connection = TDengineStmtUtils.getConnection();
        this.statement = TDengineStmtUtils.getStmt();
        lastTime = System.currentTimeMillis();
        Timer timer = new Timer();
        TimerTask processSql = new TimerTask() {
            @Override
            public void run() {
                if(!sqlDeque.isEmpty()) {
                    ConcurrentLinkedDeque<String> sqlList = new ConcurrentLinkedDeque<>();
                    StringBuffer sqlsb = new StringBuffer("INSERT INTO ");
                    ConcurrentLinkedDeque<String> processSqlDeque = sqlDeque ;
                    sqlDeque = new ConcurrentLinkedDeque<>();
                    for (String sql : processSqlDeque) {
                        sqlsb.append(sql);
                        //TDengine 支持的一条 SQL 的最大长度为 1,048,576（1MB）个字符
                        if(sqlsb.length() > 1000000){
                            sqlList.add(sqlsb.toString());
                            sqlsb = new StringBuffer("INSERT INTO ");
                        }
                    }
                    count.addAndGet(processSqlDeque.size());
                    processSqlDeque.clear();
                    sqlList.add(sqlsb.toString());
                    sqlList.forEach(sql->{
                        this.executeSQL(sql);
                    });
                }
                long now = System.currentTimeMillis();
                if (now - lastTime > 10000) {
                    lastTime = now;
                    logger.info("partition:{},过去10秒内，提交数据{}个，总计{}个" ,partition,count.get(),total.addAndGet(count.get()));
                    count.set(0);
                }
            }

            private void executeSQL(String sql) {
                try {
                    statement.executeUpdate(sql);
                    logger.info("执行SQL完成,长度：{}",sql.length());
                } catch (SQLException e) {
                    int errorCode = e.getErrorCode() & 0xffff;
                    logger.error("执行SQL错误: {} error，code:{}", sql,errorCode);
                }
            }
        };
        timer.schedule(processSql, 2000, 2000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (statement != null) {
            statement.close();
        }
    }

    @Override
    public void invoke(TdengineData tdengineData, Context context) {
        try {
            dbName = tdengineData.getTopicName().split("_")[2];
            superTableName = tdengineData.getTopicName().split("_")[3];
            partition = tdengineData.getPartition();
            StringBuffer tableNameSb = new StringBuffer();
            tdengineData.getTags().forEach((k, v) -> {
                tableNameSb.append(v).append("_");
            });
            tableName = tableNameSb.deleteCharAt(tableNameSb.length() - 1).toString();
            List<String> sqlList = genSqlList(tdengineData);
            if (CollectionUtils.isEmpty(sqlList)) {
                logger.error("sql未生成");
            } else {
                sqlList.forEach(sql -> {
                    sqlDeque.add(sql);
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<String> genSqlList(TdengineData data) {
        List<String> sqlList = new ArrayList<>();
        StringBuffer tag = new StringBuffer();
        StringBuffer tagLabel = new StringBuffer();
        try {
            if (!tableMap.containsKey(tableName) || addColumn(data, tableMap.get(tableName))) {
                describeTable(data);
            }
            data.getTags().forEach((k, v) -> {
                tag.append("'").append(v).append("',");
                tagLabel.append("'").append(k).append("',");
            });
            tagLabel.deleteCharAt(tagLabel.length() - 1);
            tag.deleteCharAt(tag.length() - 1);
        } catch (SQLException e) {
            int errorCode = e.getErrorCode() & 0xffff;
            if (errorCode == 0x2603 ||  errorCode == 0x2662) {
                logger.info("未找到子表，errorCode：{}", errorCode);
                createSTables(data);
                createTables(data);
                return genSqlList(data);
            }
        }
        StringBuffer headerSql = new StringBuffer();
        headerSql.append(dbName).append(".").append(tableName);
        StringBuffer value = new StringBuffer(String.valueOf(data.getTs())).append(",");
        StringBuffer valueLabel = new StringBuffer("ts,");
        for (int i = 0; i < data.getTagDataList().size(); i++) {
            TdengineTagData tdengineTagData = data.getTagDataList().get(i);
            valueLabel.append("`").append(tdengineTagData.getTagName()).append("`,");
            if ("DOUBLE".equals(tdengineTagData.getValueType()) || "INT".equals(tdengineTagData.getValueType())) {
                value.append(tdengineTagData.getTagValue()).append(",");
            } else {
                value.append("'").append(tdengineTagData.getTagValue()).append("',");
            }
            if (i == data.getTagDataList().size() - 1) {
                value.deleteCharAt(value.length() - 1);
                valueLabel.deleteCharAt(valueLabel.length() - 1);
                StringBuffer sql = new StringBuffer();
                sql.append(headerSql).append("(").append(valueLabel).append(")").append("VALUES (").append(value).append(")");
                value = new StringBuffer();
                valueLabel = new StringBuffer();
                sqlList.add(sql.toString());
            }
        }
        return sqlList;
    }

    private void createTables(TdengineData data) {
        StringBuffer sb = new StringBuffer("CREATE TABLE IF NOT EXISTS ");
        sb.append(dbName).append(".").append(tableName).append(" USING ").append(dbName).append(".").append(superTableName).append(" TAGS (");
        if (data.getTags().size() > 0) {
            data.getTags().forEach((k, v) -> {
                sb.append("'").append(v).append("',");
            });
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(")");
        String sql = sb.toString();
        try {
            this.statement.executeUpdate(sql);
            logger.info("创建子表完成: {}", sql);
        } catch (Throwable throwable) {
            logger.error("创建子表错误: {}", sql);
        }
    }


    private void createSTables(TdengineData data) {
        StringBuffer sb = new StringBuffer("create STABLE ");
        sb.append(dbName).append(".").append(superTableName);
        sb.append(" (`ts` TIMESTAMP,");
        for (TdengineTagData entity : data.getTagDataList()) {
            sb.append("`" + entity.getTagName() + "` ");
            sb.append("NCHAR".equals(entity.getValueType()) ? entity.getValueType() + "(100)" : entity.getValueType());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
        if (data.getTags().size() > 0) {
            sb.append(" TAGS (");
            data.getTags().forEach((k, v) -> {
                sb.append("`").append(k).append("` NCHAR(100),");
            });
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
        }
        try {
            this.statement.execute(sb.toString());
            logger.info("创建超级表完成: {}", sb.toString());
        } catch (Throwable throwable) {
        }
    }

    private void describeTable(TdengineData data) throws SQLException {
        StringBuffer describeSql = new StringBuffer("DESCRIBE ").append(dbName).append(".").append(tableName);
        List<TdengineTableData> tableDatas = null;
        try {
            PreparedStatement ps =  this.connection.prepareStatement(describeSql.toString());
            ResultSet resultSet = ps.executeQuery();
            tableDatas = new ArrayList<>();
            while (resultSet.next()) {
                TdengineTableData tableData = new TdengineTableData();
                tableData.setField(resultSet.getString(1));
                tableData.setType(resultSet.getString(2));
                tableData.setLength(resultSet.getString(3));
                tableData.setNote(resultSet.getString(4));
                tableDatas.add(tableData);
            }
        } catch (SQLException e) {
            throw e;
        }
        CaseInsensitiveMap columnMap = tableDatas.stream().filter(x -> StringUtils.isEmpty(x.getNote()) && !x.getField().equals("ts")).
                collect(Collectors.toMap(TdengineTableData::getField, (v) -> v, (a, b) -> b, CaseInsensitiveMap::new));
        tableMap.put(tableName, columnMap);
    }

    private boolean addColumn(TdengineData data, Map<String, TdengineData> columnMap) {
        AtomicReference<Boolean> result = new AtomicReference<>(false);
        data.getTagDataList().forEach(d -> {
            if (!columnMap.containsKey(d.getTagName())) {
                String sql = new StringBuffer("ALTER STABLE ").append(dbName).append(".").append(superTableName).append(" ADD COLUMN ").append("`").append(d.getTagName())
                        .append("` ").append(d.getValueType()).append("NCHAR".equals(d.getValueType()) ? "(100)" : "").toString();
                result.set(true);
                try {
                    this.statement.executeUpdate(sql);
                    logger.info("添加字段完成: {}", sql);
                } catch (Throwable throwable) {
                    logger.error("添加字段错误: {}", sql);
                }
            }
        });
        return result.get();
    }
}