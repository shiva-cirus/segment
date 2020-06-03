package io.cdap.plugin.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

public class SegmentSinkOutputformatProvider implements OutputFormatProvider {

  private final Map<String, String> configMap;


  public SegmentSinkOutputformatProvider(String operationType, String writeKey, String userId, String traits,
                                         String context, String connectionTimeout, String readTimeout,
                                         String writeTimeout){

    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(SegmentSinkConfig.PROPERTY_OPERATION_TYPE,operationType)
      .put(SegmentSinkConfig.PROPERTY_SEGMENT_WRITEKEY,writeKey)
      .put(SegmentSinkConfig.PROPERTY_SEGMENT_USERID,userId)
      .put(SegmentSinkConfig.PROPERTY_TRAITS_PROPERTIES,traits)
      .put(SegmentSinkConfig.PROPERTY_CONTEXT_PROPERTIES,context)
      .put(SegmentSinkConfig.PROPERTY_SEGEMENT_CONNECTIONTIMEOUT,connectionTimeout)
      .put(SegmentSinkConfig.PROPERTY_SEGEMENT_READTIMEOUT,readTimeout)
      .put(SegmentSinkConfig.PROPERTY_SEGEMENT_WRITETIMEOUT,writeTimeout);
    this.configMap = builder.build();
  }


  @Override
  public String getOutputFormatClassName() {
    return SegmentSinkOutformat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configMap;
  }
}
