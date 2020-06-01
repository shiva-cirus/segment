package io.cdap.plugin.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class SegmentRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
  @Override
  public void write(NullWritable nullWritable, StructuredRecord structuredRecord) throws IOException, InterruptedException {

  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

  }
}
