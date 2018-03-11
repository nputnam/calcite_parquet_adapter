package com.parquetdb.adapter.parquet;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ParquetSchema extends AbstractSchema {

    private Map<String, Table> tableMap;

    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = new HashMap<>();
            tableMap.put("example", new ParquetTable(new File(Thread.currentThread().getContextClassLoader()
            .getResource("userdata1.parquet").getPath())));
        }
        return tableMap;
    }
}
