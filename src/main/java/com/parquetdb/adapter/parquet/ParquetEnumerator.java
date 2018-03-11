package com.parquetdb.adapter.parquet;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

//TODO Here is where the predicate pushdown would be implemented. Just a predicate
// string right now representing filters. Indexing relative to the entire schema
// not the projection. Also this super simple reader doesn't even push the
// projection down the reader.
public class ParquetEnumerator implements Enumerator<Object> {

    private final ParquetReader<SimpleRecord> reader;
    private SimpleRecord current;
    private final AtomicBoolean cancel;
    private final List<RelDataTypeField> fieldTypes;

    public ParquetEnumerator(File fileToRead, AtomicBoolean cancel, RelProtoDataType protoRowType, String predicate) {
        this.cancel = cancel;
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
        try {
            this.reader = ParquetReader.builder(new SimpleReadSupport(), new Path(fileToRead.toURI())).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object current() {
        Object[] row = new Object[fieldTypes.size()];

        List<SimpleRecord.NameValue> values = current.getValues();
        int i = 0;
        outer : for (RelDataTypeField fieldType : fieldTypes) {
            String name = fieldType.getName();
            RelDataType type = fieldType.getType();
            for (SimpleRecord.NameValue value : values) {
                if (value.getName().equals(name)) {
                    row[i] = value.getValue();
                    i++;
                    continue outer;
                }
            }
            // Defaulting some values cause some are optional
            switch (type.getSqlTypeName()) {
                case DOUBLE: {
                    row[i] = 0.0D;
                    i++;
                    break;
                }
            }
        }
        return row;
    }

    @Override
    public boolean moveNext() {
        if (cancel.get()) {
            return false;
        }
        try {
            current = this.reader.read();
            if (current == null) {
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {
        try {
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
