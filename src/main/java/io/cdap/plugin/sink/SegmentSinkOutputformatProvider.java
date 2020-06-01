package io.cdap.plugin.sink;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

public class SegmentSinkOutputformatProvider implements OutputFormatProvider {
  @Override
  public String getOutputFormatClassName() {
    return SegmentSinkOutformat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return null;
  }
}
