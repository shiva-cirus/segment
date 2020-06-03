package io.cdap.plugin.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.stream.Collectors;

@Name("Segment")
@Description("Inserts records into a Segment Customer Data platform.")
@Plugin(type = BatchSink.PLUGIN_TYPE)
public class SegmentSink extends BatchSink<StructuredRecord, NullWritable, Map<String, Object>> {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentSink.class);

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
    Schema inputSchema = batchSinkContext.getInputSchema();
    FailureCollector collector = batchSinkContext.getFailureCollector();
    config.validate(inputSchema, collector);
    collector.getOrThrowException();

    batchSinkContext.addOutput(Output.of(config.getReferenceName(),
                                         new SegmentSinkOutputformatProvider(config.getConnectTimeOut(),
                                                                             config.getReadTimeOut(),
                                                                             config.getWriteTimeOut())));

    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    lineageRecorder.recordWrite("Write", "Wrote to Segment",
                                inputSchema.getFields().stream()
                                  .map(Schema.Field::getName)
                                  .collect(Collectors.toList()));
  }

/*
  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    String body = StructuredRecordStringConverter.toJsonString(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(body)));
  }

  */
}
