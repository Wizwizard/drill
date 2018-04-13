/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expand.describe;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.materialize.QueryWritableBatch;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.QueryData;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by bingxing.wang on 2018/3/7.
 */
public class ShowSchema {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ShowSchema.class);

    static Configuration conf;
    static FileSystem fs;

    public static FileStatus getStatus(Path path) throws IOException {
        FileStatus stat = fs.getFileStatus(path);
        return stat;
    }

    // 传入目录地址输出
    // 如果是文件直接输出
    public static Path getDirFile(Path path) throws IOException {
        FileStatus stat = fs.getFileStatus(path);
        if(!stat.isDirectory()) {
            return path;
        }
        FileStatus[] statu = fs.listStatus(path);
        Path[] listPaths = FileUtil.stat2Paths(statu);
        for(Path p: listPaths) {
            if(p.toString().endsWith("parquet")) {
                return p;
            }
        }

        return null;
    }

    public static long getFileLength(Path path) throws IOException {
        FileStatus stat = getStatus(path);
        return stat.getLen();
    }



    // readFrom hdfs
    // 暂时只支持标准的spark sql文件
    // 86就是无效尾部字符
    public static String readHdfsLastLine(Path pathInput, String charset) throws Exception {
//        if (!file.exists() || file.isDirectory() || !file.canRead()) {
//            return null;
//        }

        Path path = getDirFile(pathInput);

        FSDataInputStream dataInputStream = fs.open(path);
        long len = getFileLength(path);
        byte[] b = new byte[1];



        try {
            if (len == 0L) {
                return "";
            } else {
                long pos = len - 1 - 86;
                long start = System.currentTimeMillis();
                while (pos > 0) {
                    pos--;
                    dataInputStream.read(pos, b, 0, 1);
                    if (b[0] == '\n') {
                        break;
                    }
                }
                logger.info("picasso: readHdfsLastLine: seek n time: " + (System.currentTimeMillis() - start));
                if (pos == 0) {
                    dataInputStream.seek(0);
                }
                // 尾部有1+87个无用字符
                int l2read = (int) (len -pos -1 -86);
                byte[] bytes = new byte[l2read];
                dataInputStream.read(pos, bytes, 0, l2read);
                if (charset == null) {
                    return new String(bytes);
                } else {
                    return new String(bytes, charset);
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            if (dataInputStream != null) {
                try {
                    dataInputStream.close();
                } catch (Exception e2) {
                    throw e2;
                }
            }
        }
    }
    public static String getSchemaString (String path) throws Exception {


        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, getDirFile(new Path(path)));
        Map<String, String> schema = readFooter.getFileMetaData().getKeyValueMetaData();
        return schema.get("org.apache.spark.sql.parquet.row.metadata");
    }
    public static SchemaResult getSchema (String path) throws Exception {

        //ParquetMetadata

        SchemaResult schema = new SchemaResult();


        try {
//            conf = new Configuration();
//            conf.set("fs.defaultFS", connection);
//            fs = FileSystem.get(conf);
//            String line = readHdfsLastLine(new Path(path), null);
            conf = new Configuration();
            conf.addResource(ShowSchema.class.getResource("/../conf/core-site.xml"));
            conf.addResource(ShowSchema.class.getResource("/../conf/hdfs-site.xml"));
            fs = FileSystem.get(conf);
            String line = getSchemaString(path);
            //logger.info("picasso: getSchema: line: " + line);
            int s = line.indexOf("fields") + 8;
            int e = line.length() - 1;
            //int e = line.indexOf("]}") + 1;
            JsonParser jsonParser = new JsonParser();
            //JsonArray jsonArray = jsonParser.parse(line.substring(s, e)).getAsJsonArray();
            JsonArray jsonArray = jsonParser.parse(line.substring(s, e)).getAsJsonArray();

            StringBuilder sb = new StringBuilder();
            sb.append("{\n");

            String name;
            String type;
            for(int i = 0; i < jsonArray.size(); i ++){
                JsonObject jsonObject =  (JsonObject)jsonArray.get(i);
                Map<String, String> map = new HashMap<>();
                name = jsonObject.get("name").toString();
                type = jsonObject.get("type").toString();
                name = name.substring(1, name.length() - 1);
                if(!type.startsWith("{")) {
                    type = type.substring(1, type.length() - 1);
                }
                map.put("COLUMN_NAME", name);
                map.put("DATA_TYPE", type);

//                sb.append("name: " + name + "\n");
//                sb.append("type: " + type + "\n");
//                sb.append("----------\n");

                schema.results.add(map);
            }
            sb.append("}");
            //logger.info("picasso: getSchema: schema:\n" + sb);
        } catch (IOException e) {
            throw e;
        } finally {
            //fs.close();
        }

        return schema;
    }

    public static QueryWritableBatch getClientSchema(String path, QueryId queryId) throws Exception {

        SchemaResult schemaResult = getSchema(path);
        int resSize = schemaResult.results.size();

        StringBuilder columnNames = new StringBuilder();
        StringBuilder types = new StringBuilder();
        int[] columnOffsets = new int[resSize + 1];
        int[] typeOffsets = new int[resSize + 1];
        columnOffsets[0] = 0;
        typeOffsets[0] = 0;
        int index = 1;

        for(Map<String, String> result : schemaResult.results){
            columnNames.append(result.get("COLUMN_NAME"));
            types.append(result.get("DATA_TYPE"));
            columnOffsets[index] = columnOffsets[index - 1] + result.get("COLUMN_NAME").length();
            typeOffsets[index] = typeOffsets[index - 1] + result.get("DATA_TYPE").length();
            index ++;
        }


        SerializedField offsets = SerializedField.newBuilder()
                .setMajorType(TypeProtos.MajorType.newBuilder()
                    .setMinorType(TypeProtos.MinorType.UINT4).build())
                .setNamePart(UserBitShared.NamePart.newBuilder()
                    .setName("$offsets$").build())
                .setValueCount(resSize + 1)
                .setBufferLength((resSize + 1) * 4)
                .build();
        SerializedField bits = SerializedField.newBuilder()
                .setMajorType(TypeProtos.MajorType.newBuilder()
                    .setMinorType(TypeProtos.MinorType.UINT1).build())
                .setNamePart(UserBitShared.NamePart.newBuilder()
                    .setName("$bits$").build())
                .setValueCount(resSize)
                .setBufferLength(resSize * 1)
                .build();

        // 构建column列
        SerializedField column2 = SerializedField.newBuilder()
                .setMajorType(TypeProtos.MajorType.newBuilder()
                    .setMinorType(TypeProtos.MinorType.VARCHAR).build())
                .setNamePart(UserBitShared.NamePart.newBuilder()
                    .setName("COLUMN_NAME").build())
                .addChild(offsets)
                .setValueCount(resSize)
                .setBufferLength(columnNames.toString().length() + offsets.getBufferLength())
                .build();
        SerializedField column1 = SerializedField.newBuilder()
                .setMajorType(TypeProtos.MajorType.newBuilder()
                        .setMinorType(TypeProtos.MinorType.VARCHAR).build())
                .setNamePart(UserBitShared.NamePart.newBuilder()
                        .setName("COLUMN_NAME").build())
                .addChild(bits)
                .addChild(column2)
                .setValueCount(resSize)
                .setBufferLength(bits.getBufferLength() + column2.getBufferLength())
                .build();

        // 构建type列
        SerializedField type2 = SerializedField.newBuilder()
                .setMajorType(TypeProtos.MajorType.newBuilder()
                        .setMinorType(TypeProtos.MinorType.VARCHAR).build())
                .setNamePart(UserBitShared.NamePart.newBuilder()
                        .setName("DATA_TYPE").build())
                .addChild(offsets)
                .setValueCount(resSize)
                .setBufferLength(offsets.getBufferLength() + types.toString().length())
                .build();
        SerializedField type1 = SerializedField.newBuilder()
                .setMajorType(TypeProtos.MajorType.newBuilder()
                        .setMinorType(TypeProtos.MinorType.VARCHAR).build())
                .setNamePart(UserBitShared.NamePart.newBuilder()
                        .setName("DATA_TYPE").build())
                .addChild(bits)
                .addChild(column2)
                .setValueCount(resSize)
                .setBufferLength(bits.getBufferLength() + type2.getBufferLength())
                .build();

        //构建def
        UserBitShared.RecordBatchDef def = UserBitShared.RecordBatchDef.newBuilder()
                .setRecordCount(resSize)
                .addField(column1)
                .addField(type1)
                .setCarriesTwoByteSelectionVector(false)
                .build();

        QueryData header = QueryData.newBuilder()
                .setQueryId(queryId)
                .setRowCount(resSize)
                .setDef(def)
                .build();

        ByteBuf[] buffers = new ByteBuf[schemaResult.columns.size() * 3];
        StringBuilder str = new StringBuilder();
        byte[] bytes = new byte[offsets.getBufferLength()];

        for(int i = 0; i < buffers.length; i ++){
            buffers[i] = Unpooled.buffer();
        }

        // 生成对应数量的char(1)
        for(int i = 0; i < resSize; i++){
            str.append((char)1);
        }
        buffers[0].writeBytes(str.toString().getBytes());

        //TODO 这里要处理大端小端问题...
        for(int i = 0; i < bytes.length; i += 4) {
           bytes[i+3] = (byte) ((columnOffsets[i / 4] >> 24) & 0xff);
           bytes[i+2] = (byte) ((columnOffsets[i / 4] >> 16) & 0xff);
           bytes[i+1] = (byte) ((columnOffsets[i / 4] >> 8) & 0xff);
           bytes[i+0] = (byte) ((columnOffsets[i / 4] >> 0) & 0xff);
        }
        buffers[1].writeBytes(bytes);

        buffers[2].writeBytes(columnNames.toString().getBytes());
        buffers[3] = buffers[0];
        bytes = new byte[offsets.getBufferLength()];
        for(int i = 0; i < bytes.length; i += 4) {
            bytes[i+3] = (byte) ((typeOffsets[i / 4] >> 24) & 0xff);
            bytes[i+2] = (byte) ((typeOffsets[i / 4] >> 16) & 0xff);
            bytes[i+1] = (byte) ((typeOffsets[i / 4] >> 8) & 0xff);
            bytes[i+0] = (byte) ((typeOffsets[i / 4] >> 0) & 0xff);
        }
        buffers[4].writeBytes(bytes);
        buffers[5].writeBytes(types.toString().getBytes());

        return new QueryWritableBatch(header, buffers);

    }




    //read local file
    public static String readLastLine(File file, String charset) throws IOException {
        if (!file.exists() || file.isDirectory() || !file.canRead()) {
            return null;
        }
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "r");
            long len = raf.length();
            if (len == 0L) {
                return "";
            } else {
                long pos = len - 1;
                while (pos > 0) {
                    pos--;
                    raf.seek(pos);
                    if (raf.readByte() == '\n') {
                        break;
                    }
                }
                if (pos == 0) {
                    raf.seek(0);
                }
                byte[] bytes = new byte[(int) (len - pos)];
                raf.read(bytes);
                if (charset == null) {
                    return new String(bytes);
                } else {
                    return new String(bytes, charset);
                }
            }
        } catch (FileNotFoundException e) {
        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (Exception e2) {
                }
            }
        }
        return null;
    }
}
