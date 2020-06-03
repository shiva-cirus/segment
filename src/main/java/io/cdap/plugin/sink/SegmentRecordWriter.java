package io.cdap.plugin.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.plugin.common.SegmentClient;
import io.cdap.plugin.common.SegmentOperationType;
import io.cdap.plugin.common.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class SegmentRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentRecordWriter.class);
  private final String operationType;
  private final String writeKey;
  private final String userId;
  private final String traitsMappings;
  private final String contextMappings;
  private final int connectTimeout;
  private final int readTimeout;
  private final int writeTimeout;

  public SegmentRecordWriter(TaskAttemptContext taskAttemptContext){
    Configuration config = taskAttemptContext.getConfiguration();
    this.operationType = config.get(SegmentSinkConfig.PROPERTY_OPERATION_TYPE);
    this.writeKey = config.get(SegmentSinkConfig.PROPERTY_SEGMENT_WRITEKEY);
    this.userId = config.get(SegmentSinkConfig.PROPERTY_SEGMENT_USERID);
    this.traitsMappings = config.get(SegmentSinkConfig.PROPERTY_TRAITS_PROPERTIES);
    this.contextMappings = config.get(SegmentSinkConfig.PROPERTY_CONTEXT_PROPERTIES);
    this.connectTimeout = config.getInt(SegmentSinkConfig.PROPERTY_SEGEMENT_CONNECTIONTIMEOUT,15);
    this.readTimeout = config.getInt(SegmentSinkConfig.PROPERTY_SEGEMENT_READTIMEOUT,15);
    this.writeTimeout = config.getInt(SegmentSinkConfig.PROPERTY_SEGEMENT_WRITETIMEOUT,15);
  }




  @Override
  public void write(NullWritable nullWritable, StructuredRecord structuredRecord) throws IOException, InterruptedException {


    // Using the Structured Record get the Value of Write Key
    String writeKeyVal = getValue(structuredRecord::get, writeKey, "String", String.class);
    LOG.debug("Processing record with value = "+writeKeyVal);
    SegmentClient client = SegmentClient.getInstance(writeKeyVal,connectTimeout,readTimeout,writeTimeout);
    if (SegmentOperationType.IDENTIFY == SegmentOperationType.fromValue(operationType).get()){
      String userIdVal = getValue(structuredRecord::get, userId, "String", String.class);
      Map<String,String> traitsKVMappings = StringUtil.parseKeyValueConfig(traitsMappings, ";", "=");
      Map<String,String> traits = new HashMap<String,String>();
      for (Map.Entry<String,String> entry : traitsKVMappings.entrySet()) {
        String val = getValue(structuredRecord::get, entry.getValue(), "String", String.class);
        traits.put(entry.getKey(), val);
      }
      Map<String,String> contextKVMappings = StringUtil.parseKeyValueConfig(contextMappings, ";", "=");
      Map<String,String> context = new HashMap<String,String>();
      for (Map.Entry<String,String> entry : contextKVMappings.entrySet()) {
        String val = getValue(structuredRecord::get, entry.getValue(), "String", String.class);
        context.put(entry.getKey(), val);
      }
      client.identify(userIdVal,traits,context);
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    // Here forcefully flush and wait till its completed.
      SegmentClient.flushAll();

  }


  private <T> T getValue(Function<String, T> valueExtractor, String fieldName, String fieldType, Class<T> clazz) {
    T value = valueExtractor.apply(fieldName);
    if (clazz.isAssignableFrom(value.getClass())) {
      return clazz.cast(value);
    }
    throw new UnexpectedFormatException(
      String.format("Field '%s' is not of expected type '%s'", fieldName, fieldType));
  }





}
