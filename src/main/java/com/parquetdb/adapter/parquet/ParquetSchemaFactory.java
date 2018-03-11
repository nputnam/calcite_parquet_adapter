package com.parquetdb.adapter.parquet;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class ParquetSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {
        return new ParquetSchema();
    }
}
