package io.cdap.plugin.sink;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

public class SegmentSinkOutputformatProvider implements OutputFormatProvider {

  private final Map<String, Integer> configMap;


  public SegmentSinkOutputformatProvider(int connectTimeOut, int readTimeOut, int writeTimeOut){

    ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<String, Integer>()
      .put(SegmentSinkConfig.PROPERTY_SEGEMENT_CONNECTIONTIMEOUT,connectTimeOut)
      .put(SegmentSinkConfig.PROPERTY_SEGEMENT_READTIMEOUT,readTimeOut)
      .put(SegmentSinkConfig.PROPERTY_SEGEMENT_WRITETIMEOUT,writeTimeOut);
    this.configMap = builder.build();
  }


  @Override
  public String getOutputFormatClassName() {
    return SegmentSinkOutformat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return null;
  }
}
