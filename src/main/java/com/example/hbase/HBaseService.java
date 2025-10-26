package com.example.hbase;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;

@Service
public class HBaseService {

    private final Configuration configuration;
    private Connection connection;

    public HBaseService(Configuration configuration) {
        this.configuration = configuration;
    }

    @PostConstruct
    public void init() throws IOException {
        connection = ConnectionFactory.createConnection(configuration);
    }

    @PreDestroy
    public void close() throws IOException {
        if (connection != null) connection.close();
    }

    public String getValue(String tableName, String rowKey, String family, String qualifier) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        table.close();
        return value == null ? null : Bytes.toString(value);
    }
}