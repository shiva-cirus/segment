package io.cdap.plugin.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import io.cdap.cdap.format.StructuredRecordStringConverter;

import java.util.Map;

@Name("Segment")
@Description("Inserts records into a Segment Customer Data platform.")
@Plugin(type = BatchSink.PLUGIN_TYPE)
public class SegmentSink extends BatchSink<StructuredRecord, NullWritable, Map<String, Object>> {

  private final SegmentSinkConfig config;

  public SegmentSink(SegmentSinkConfig config){ this.config = config;}


  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema input = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(input,collector);
  }

  @Override
  public void prepareRun(BatchSinkContext batchSinkContext) throws Exception {

  }


  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    String body = StructuredRecordStringConverter.toJsonString(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(body)));
  }
}
