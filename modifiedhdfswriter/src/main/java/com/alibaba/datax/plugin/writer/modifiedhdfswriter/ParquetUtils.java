package com.alibaba.datax.plugin.writer.modifiedhdfswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.io.api.Binary;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import java.io.IOException;
import java.util.List;

/**
 * ClassName： ParquetUtils
 * Description： Parquet Util
 *
 * @author 0x3E6
 * @version 1.0.0
 * @date 6/2/20 5:38 PM
 */
public class ParquetUtils {
    // 每天的纳秒数
    private static final long NANO_SECONDS_PER_DAY = 86400_000_000_000L;
    // 儒略历起始日（儒略历的公元前4713年1月1日中午12点，在格里历是公元前4714年11月24日）距离1970-01-01的天数
    private static final long JULIAN_EPOCH_OFFSET_DAYS = 2440588;
    private static final Logger LOG = LoggerFactory.getLogger(ParquetUtils.class);

    public String constructSchema(List<Configuration> columns) {
        StringBuilder schema = new StringBuilder("message pair {\n");
        for (Configuration eachColumnConf : columns) {
            schema.append(" optional ");
            SupportHiveDataType columnType = SupportHiveDataType.valueOf(eachColumnConf.getString(Key.TYPE).toUpperCase());
            String columnName = eachColumnConf.getString(Key.NAME);
            schema.append(getColumnDef(columnType, columnName, eachColumnConf));
            schema.append(";\n");
        }
        schema.append("}");
        return schema.toString();
    }

    public ParquetWriter<Group> getParquetWriter(String schema, String fileName,
                                                 org.apache.hadoop.conf.Configuration configuration) throws IOException {
        MessageType messageType = MessageTypeParser.parseMessageType(schema);
        Path path = new Path(fileName);
        GroupWriteSupport.setSchema(messageType, configuration);
        return new ParquetWriter<>(path, configuration, new GroupWriteSupport());
    }

    private String getColumnDef(SupportHiveDataType columnType, String column, Configuration eachColumnConf) {
        String type;
        switch (columnType) {
            case TINYINT:
            case SMALLINT:
            case INT:
                type = "int32 " + column;
                break;
            case DATE:
            case TIMESTAMP:
                type = "int96 " + column;
                break;
            case BIGINT:
                type = "int64 " + column;
                break;
            case FLOAT:
                type = "float " + column;
                break;
            case DOUBLE:
                type = "double " + column;
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                type = "binary " + column + "(UTF8)";
                break;
            case BOOLEAN:
                type = "boolean " + column;
                break;
            default:
                throw DataXException
                        .asDataXException(
                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                        eachColumnConf.getString(Key.NAME),
                                        eachColumnConf.getString(Key.TYPE)));
        }
        return type;
    }

    public MutablePair<Group, Boolean> transportOneRecord(GroupFactory factory, Record record,
            List<Configuration> columnsConf, TaskPluginCollector taskPluginCollector) {
        MutablePair<Group, Boolean> transportResult = new MutablePair<>();
        transportResult.setRight(false);
        int recordLength = record.getColumnNumber();
        if (0 == recordLength) {
            transportResult.setRight(true);
            return transportResult;
        }
        Group group = factory.newGroup();
        Column column;
        for (int i = 0; i < recordLength; i++) {
            column = record.getColumn(i);
            if (column.getRawData() != null) {
                try {
                    parseData(group, column, columnsConf.get(i).getString(Key.NAME), columnsConf.get(i).getString(Key.TYPE));
                } catch (Exception e) {
                    transportResult.setRight(true);
                    break;
                }
            }
        }
        transportResult.setLeft(group);
        return transportResult;
    }

    private void parseData(Group group, Column column, String columnName, String columnType) {
        String rawData = column.getRawData().toString();
        SupportHiveDataType hiveType = SupportHiveDataType.valueOf(columnType.toUpperCase());
        //根据目标列类型进行转换
        switch (hiveType) {
            case TINYINT:
                group.append(columnName, Byte.valueOf(rawData));
                break;
            case SMALLINT:
                group.append(columnName, Short.valueOf(rawData));
                break;
            case INT:
                group.append(columnName, Integer.valueOf(rawData));
                break;
            case BIGINT:
                group.append(columnName, column.asLong());
                break;
            case FLOAT:
                group.append(columnName, Float.valueOf(rawData));
                break;
            case DOUBLE:
                group.append(columnName, column.asDouble());
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                group.append(columnName, column.asString());
                break;
            case BOOLEAN:
                group.append(columnName, column.asBoolean());
                break;
            case DATE:
            case TIMESTAMP:
                group.append(columnName, getInt96(column.asDate().getTime()));
                break;
            default:
                throw DataXException
                        .asDataXException(
                                HdfsWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                        columnName, columnType));
        }
    }

    // 调转字节数组
    public static void flip(byte[] bytes) {
        for(int i = 0,j = bytes.length - 1; i < j; i++, j--) {
            byte t = bytes[i];
            bytes[i] = bytes[j];
            bytes[j] = t;
        }
    }

    private Binary getInt96(long dataTime){
        // 转换成距1970-01-01 00:00:00的纳秒数
        long nano = dataTime * 1000_000;
        // 转换成距儒略历起始日的天数
        int julianDays = (int) ((nano / NANO_SECONDS_PER_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
        byte[] julianDaysBytes = getBytes(julianDays);
        flip(julianDaysBytes);
        // 当前时间戳距离当天已过去的纳秒数
        long lastDayNanos = nano % NANO_SECONDS_PER_DAY;
        byte[] lastDayNanosBytes = getBytes(lastDayNanos);
        flip(lastDayNanosBytes);
        byte[] dst = new byte[12];
        // 前8字节表示时间戳对应当天已过去的纳秒数
        System.arraycopy(lastDayNanosBytes, 0, dst, 0, 8);
        // 后4字节表示时间戳当天距离儒略历起始日已过去的天数
        System.arraycopy(julianDaysBytes, 0, dst, 8, 4);
        return Binary.fromByteArray(dst);
    }

    public static byte[] getBytes(int i) {
        byte[] bytes=new byte[4];
        bytes[0]=(byte)((i >> 24) & 0xFF);
        bytes[1]=(byte)((i >> 16) & 0xFF);
        bytes[2]=(byte)((i >> 8) & 0xFF);
        bytes[3]=(byte)(i & 0xFF);
        return bytes;
    }

    public static byte[] getBytes(long i) {
        byte[] bytes=new byte[8];
        bytes[0]=(byte)((i >> 56) & 0xFF);
        bytes[1]=(byte)((i >> 48) & 0xFF);
        bytes[2]=(byte)((i >> 40) & 0xFF);
        bytes[3]=(byte)((i >> 32) & 0xFF);
        bytes[4]=(byte)((i >> 24) & 0xFF);
        bytes[5]=(byte)((i >> 16) & 0xFF);
        bytes[6]=(byte)((i >> 8) & 0xFF);
        bytes[7]=(byte)(i & 0xFF);
        return bytes;
    }
}