package io.cdap.plugin.sink;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.LineageRecorder;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

@Name("Segment")
@Description("Inserts records into a Segment Customer Data platform.")
@Plugin(type = BatchSink.PLUGIN_TYPE)
public class SegmentSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
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

    String operationType = config.getOperationType();
    String writeKey = config.getWriteKey();
    String userId = config.getUserId();
    String traits = config.getTraitsMappings();
    String context = config.getContextMappings();
    String connectionTimeout = Integer.toString(config.getConnectTimeOut());
    String readTimeout = Integer.toString(config.getReadTimeOut());
    String writeTimeout = Integer.toString(config.getWriteTimeOut());

    batchSinkContext.addOutput(Output.of(config.getReferenceName(),
                                         new SegmentSinkOutputformatProvider(operationType,writeKey,userId,traits,
                                                                             context,connectionTimeout,readTimeout,
                                                                             writeTimeout)));

    LineageRecorder lineageRecorder = new LineageRecorder(batchSinkContext, config.getReferenceName());
    lineageRecorder.createExternalDataset(inputSchema);
    // Record the field level WriteOperation
    lineageRecorder.recordWrite("Write", "Wrote to Segment",
                                inputSchema.getFields().stream()
                                  .map(Schema.Field::getName)
                                  .collect(Collectors.toList()));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, StructuredRecord>> emitter) {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }
}
