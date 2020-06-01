/*
 * Copyright © 2016-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;

import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.FileSet;
import io.cdap.cdap.api.dataset.lib.KeyValueTable;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.batch.BatchAggregator;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.PostAction;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkSink;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import io.cdap.plugin.sink.SegmentSink;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class PipelineTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      SegmentSink.class);
  }



  @Test
  public void SegmentSink() throws Exception {
    // create the pipeline config
    String inputName = "sinkTestInput";
    String outputName = "sinkTestOutput";
    String outputDirName = "users";

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputName));

    Map<String, String> sinkProperties = new HashMap<>();
    sinkProperties.put(TextFileSetSink.Conf.FILESET_NAME, outputName);
    sinkProperties.put(TextFileSetSink.Conf.FIELD_SEPARATOR, "|");
    sinkProperties.put(TextFileSetSink.Conf.OUTPUT_DIR, "${dir}");
    ETLStage sink = new ETLStage("sink", new ETLPlugin(TextFileSetSink.NAME, BatchSink.PLUGIN_TYPE,
                                                       sinkProperties, null));

    Map<String, String> actionProperties = new HashMap<>();
    actionProperties.put(FilesetDeletePostAction.Conf.FILESET_NAME, outputName);
    // mapreduce writes multiple files to the output directory. Along with the actual output,
    // there are various .crc files that do not contain any of the output content.
    actionProperties.put(FilesetDeletePostAction.Conf.DELETE_REGEX, ".*\\.crc|_SUCCESS");
    actionProperties.put(FilesetDeletePostAction.Conf.DIRECTORY, outputDirName);
    ETLStage postAction = new ETLStage("cleanup", new ETLPlugin(FilesetDeletePostAction.NAME, PostAction.PLUGIN_TYPE,
                                                                actionProperties, null));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addPostAction(postAction)
      .addConnection(source.getName(), sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("textSinkTestPipeline");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));

    // write some data to the input fileset
    Schema inputSchema = Schema.recordOf("test",
                                         Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                         Schema.Field.of("item", Schema.of(Schema.Type.STRING)));

    Map<String, String> users = new HashMap<>();
    users.put("samuel", "wallet");
    users.put("dwayne", "rock");
    users.put("christopher", "cowbell");

    List<StructuredRecord> inputRecords = new ArrayList<>();
    for (Map.Entry<String, String> userEntry : users.entrySet()) {
      String name = userEntry.getKey();
      String item = userEntry.getValue();
      inputRecords.add(StructuredRecord.builder(inputSchema).set("name", name).set("item", item).build());
    }
    DataSetManager<Table> inputManager = getDataset(inputName);
    MockSource.writeInput(inputManager, inputRecords);

    // run the pipeline
    Map<String, String> runtimeArgs = new HashMap<>();
    // the ${dir} macro will be substituted with "users" for our pipeline run
    runtimeArgs.put("dir", outputDirName);

    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start(runtimeArgs);
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 4, TimeUnit.MINUTES);

    // check the pipeline output
    DataSetManager<FileSet> outputManager = getDataset(outputName);
    FileSet output = outputManager.get();
    Location outputDir = output.getBaseLocation().append(outputDirName);

    Map<String, String> actual = new HashMap<>();
    for (Location outputFile : outputDir.list()) {
      if (outputFile.getName().endsWith(".crc") || "_SUCCESS".equals(outputFile.getName())) {
        Assert.fail("Post action did not delete file " + outputFile.getName());
      }

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(outputFile.getInputStream()))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] parts = line.split("\\|");
          actual.put(parts[0], parts[1]);
        }
      }
    }

    Assert.assertEquals(actual, users);
  }

}
