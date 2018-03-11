package com.parquetdb.adapter.parquet;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.*;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.*;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ParquetTable extends AbstractQueryableTable implements  TranslatableTable {

    private final File file;

    public ParquetTable(File rootDir) {
        super(Object[].class);
        this.file = rootDir;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        try {
            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(new Configuration(), new Path(file.toURI()), ParquetMetadataConverter.NO_FILTER);
            MessageType schema = parquetMetadata.getFileMetaData().getSchema();
            List<Type> fields = schema.getFields();
            final List<RelDataType> types = new ArrayList<>();
            final List<String> names = new ArrayList<>();
            for (Type field : fields) {
                names.add(field.getName());
                PrimitiveType.PrimitiveTypeName primitiveTypeName = field.asPrimitiveType().getPrimitiveTypeName();
                switch (primitiveTypeName) {
                    case INT96: {
                        types.add(typeFactory.createSqlType(SqlTypeName.BIGINT));
                        break;
                    }
                    case INT32: {
                        types.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
                        break;
                    }
                    case FLOAT: {
                        types.add(typeFactory.createSqlType(SqlTypeName.FLOAT));
                        break;
                    }
                    case BINARY: {
                        types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
                        break;
                    }
                    case DOUBLE: {
                        types.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
                        break;
                    }
                }
            }
            return typeFactory.createStructType(Pair.zip(names, types));

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public Enumerable<Object> runQuery(final List<String> fields, final String predicate) {
        final RelDataTypeFactory typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        final RelDataType rowType = getRowType(typeFactory);

        if (fields.isEmpty()) {
            for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
                fieldInfo.add(relDataTypeField);
            }
        } else {
            for (String field : fields) {
                fieldInfo.add(rowType.getField(field, true, false));
            }
        }
        final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

        return new AbstractEnumerable<Object>() {
            public Enumerator<Object> enumerator() {
                return new ParquetEnumerator(file, new AtomicBoolean(false), resultRowType, predicate);
            }
        };
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        List<String> fieldNames = relOptTable.getRowType().getFieldNames();
        return new ParquetTableScan(context.getCluster(), context.getCluster().traitSetOf(ParquetRel.CONVENTION),
                relOptTable, this, relOptTable.getRowType());
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        throw new UnsupportedOperationException();
    }

}
