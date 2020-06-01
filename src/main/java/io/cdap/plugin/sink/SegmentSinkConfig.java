package io.cdap.plugin.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import javax.annotation.Nullable;

public class SegmentSinkConfig extends PluginConfig {


  public void validate(@Nullable Schema inputSchema, FailureCollector collector) {

  }
}
