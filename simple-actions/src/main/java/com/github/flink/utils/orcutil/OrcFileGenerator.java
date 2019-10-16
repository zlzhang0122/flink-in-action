package com.github.flink.utils.orcutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

/**
 * for generator orc file
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/16 5:45 PM
 */
public class OrcFileGenerator {
    public static void main(String[] args) throws Exception{
        Path path = new Path("/user/zhangjiao/orc/test.orc");

        Configuration conf = new Configuration();
        TypeDescription schema = TypeDescription.fromString("struct<id:int,name:int,count:int>");
        Writer writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf).setSchema(schema)
                .compress(CompressionKind.SNAPPY));
        VectorizedRowBatch batch = schema.createRowBatch();
        LongColumnVector first = (LongColumnVector)batch.cols[0];
        LongColumnVector second = (LongColumnVector)batch.cols[1];
        LongColumnVector third = (LongColumnVector)batch.cols[2];

        final int BATCH_SIZE = batch.getMaxSize();
        for(int r = 0; r< 3; r++){
            int row = batch.size++;
            first.vector[row] = r;
            second.vector[row] = r * 3;
            third.vector[row] = r * 6;

            if(row == BATCH_SIZE - 1){
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }

        writer.close();
    }
}
