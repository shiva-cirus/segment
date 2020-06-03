package io.cdap.plugin.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SegmentRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRecordWriter.class);
  private final int connectTimeout;
  private final int readTimeout;
  private final int writeTimeout;

  public SegmentRecordWriter(TaskAttemptContext taskAttemptContext){
    Configuration config = taskAttemptContext.getConfiguration();
    this.connectTimeout = config.getInt(SegmentSinkConfig.PROPERTY_SEGEMENT_CONNECTIONTIMEOUT,15);
    this.readTimeout = config.getInt(SegmentSinkConfig.PROPERTY_SEGEMENT_READTIMEOUT,15);
    this.writeTimeout = config.getInt(SegmentSinkConfig.PROPERTY_SEGEMENT_WRITETIMEOUT,15);
  }




  @Override
  public void write(NullWritable nullWritable, StructuredRecord structuredRecord) throws IOException, InterruptedException {



  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

  }
}
