package io.cdap.plugin.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.SegmentOperationType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

public class SegmentSinkConfig extends PluginConfig {

  public static final String PROPERTY_OPERATION_TYPE = "operationType";
  public static final String PROPERTY_SEGMENT_WRITEKEY = "writeKey";
  public static final String PROPERTY_SEGMENT_USERID = "userId";
  public static final String PROPERTY_TRAITS_PROPERTIES = "traitsMappings";
  public static final String PROPERTY_CONTEXT_PROPERTIES = "contextMappings";
  public static final String PROPERTY_SEGEMENT_CONNECTIONTIMEOUT = "connectTimeOut";
  public static final String PROPERTY_SEGEMENT_READTIMEOUT = "readTimeOut";
  public static final String PROPERTY_SEGEMENT_WRITETIMEOUT = "writeTimeOut";
  private static final int MAX_TIMEOUT = 60;

  public String getReferenceName() {
    return referenceName;
  }

  public SegmentOperationType getOperationType(FailureCollector collector) {
    SegmentOperationType operationType = getOperationType();
    if (operationType != null) {
      return operationType;
    }

    collector.addFailure("Unsupported Operation type value: " + operationType,
                         String.format("Supported types are: %s", SegmentOperationType.getSupportedTypes()))
      .withConfigProperty(PROPERTY_OPERATION_TYPE);
    collector.getOrThrowException();
    return null;
  }

  public SegmentOperationType getOperationType() {
    Optional<SegmentOperationType> soperationType = SegmentOperationType.fromValue(operationType);

    return soperationType.isPresent() ? soperationType.get() : null;
  }


  public String getUserId() {
    return userId;
  }

  public Map<String, String> getTraitsMappings() {
    return Strings.isNullOrEmpty(traitsMappings) ? Collections.emptyMap() :
      parseKeyValueConfig(traitsMappings, ",", "=");
  }


  @Nullable
  public Map<String, String> getContextMappings() {
    return Strings.isNullOrEmpty(contextMappings) ? Collections.emptyMap() :
      parseKeyValueConfig(contextMappings, ",", "=");
  }

  public int getConnectTimeOut() {
    return connectTimeOut;
  }

  public int getReadTimeOut() {
    return readTimeOut;
  }

  public int getWriteTimeOut() {
    return writeTimeOut;
  }


  public String getWriteKey() {
    return writeKey;
  }


  @Name("referenceName")
  @Description("This will be used to uniquely identify this sink for lineage, annotating metadata, etc.")
  protected String referenceName;


  @Name(PROPERTY_OPERATION_TYPE)
  @Description("Segment Operation Type.")
  @Macro
  private String operationType;

  @Name(PROPERTY_SEGMENT_WRITEKEY)
  @Description("Input Schema field containing writeKey.")
  @Macro
  private String writeKey;


  @Name(PROPERTY_SEGMENT_USERID)
  @Description("Input Schema field containing userID.")
  @Macro
  private String userId;

  @Name(PROPERTY_TRAITS_PROPERTIES)
  @Description("Input Schema fields to be passed as Traits properties.")
  @Macro
  @Nullable
  private String traitsMappings;

  @Name(PROPERTY_CONTEXT_PROPERTIES)
  @Description("Input Schema fields to be passed as Context properties.")
  @Nullable
  @Macro
  private String contextMappings;

  @Name(PROPERTY_SEGEMENT_CONNECTIONTIMEOUT)
  @Macro
  @Description("Max Time in sec's to wait for connection")
  private int connectTimeOut;

  @Name(PROPERTY_SEGEMENT_READTIMEOUT)
  @Macro
  @Description("Max Time in sec's to wait for Read operation")
  private int readTimeOut;


  @Name(PROPERTY_SEGEMENT_WRITETIMEOUT)
  @Macro
  @Description("Max Time in sec's to wait for Write operation")
  private int writeTimeOut;


  /*
   * Constructor required for Initialization
   */
  public SegmentSinkConfig() {

  }

  /*
   * Constructor
   */

  public SegmentSinkConfig(String referenceName, String operationType, String writeKey, String userId, @Nullable  String traitsMappings, @Nullable  String contextMappings,
                           int connectTimeOut, int readTimeOut, int writeTimeOut) {
    this.referenceName = referenceName;
    this.operationType = operationType;
    this.writeKey = writeKey;
    this.userId = userId;
    this.traitsMappings = traitsMappings;
    this.contextMappings = contextMappings;
    this.connectTimeOut = connectTimeOut;
    this.readTimeOut = readTimeOut;
    this.writeTimeOut = writeTimeOut;
  }


  public void validate(@Nullable Schema inputSchema, FailureCollector collector) {
    validateConnectionTimeout(collector);
    validateReadTimeout(collector);
    validateWriteTimeout(collector);
    if (inputSchema != null) {
      // Check if userid field exists in Input Schema
      validateUserID(inputSchema, collector);
      // Check if Traits properties in Input Schema
      validateTraits(inputSchema, collector);
      // Check if Context Properties in Input Schma
      validateContext(inputSchema, collector);
    }
  }

  private void validateUserID(Schema inputSchema, FailureCollector collector) {
    if (containsMacro(PROPERTY_SEGMENT_USERID)) {
      return;
    }

    if (inputSchema.getField(userId) == null) {
      collector.addFailure(String.format("Invalid field name  %s specified.", userId),
                           String.format("Ensure the field is defined in Input schema."))
        .withConfigProperty(PROPERTY_SEGMENT_USERID);
    }

  }

  private void validateTraits(Schema inputSchema, FailureCollector collector) {
    if (containsMacro(PROPERTY_SEGMENT_USERID)) {
      return;
    }

    Map<String, String> traits = getTraitsMappings();
    for (String fieldName : traits.values()) {
      if (inputSchema.getField(fieldName) == null) {
        collector.addFailure(String.format("Invalid field name  %s specified.", userId),
                             String.format("Ensure the field is defined in Input schema."))
          .withConfigProperty(PROPERTY_TRAITS_PROPERTIES);

      }
    }


  }


  private void validateContext(Schema inputSchema, FailureCollector collector) {
    if (containsMacro(PROPERTY_SEGMENT_USERID)) {
      return;
    }
    Map<String, String> context = getContextMappings();
    for (String fieldName : context.values()) {
      if (inputSchema.getField(fieldName) == null) {
        collector.addFailure(String.format("Invalid field name  %s specified.", userId),
                             String.format("Ensure the field is defined in Input schema."))
          .withConfigProperty(PROPERTY_CONTEXT_PROPERTIES);

      }
    }

  }


  private void validateConnectionTimeout(FailureCollector collector) {
    if (containsMacro(PROPERTY_SEGEMENT_CONNECTIONTIMEOUT)) {
      return;
    }
    if (connectTimeOut < 1 || connectTimeOut > MAX_TIMEOUT) {
      collector.addFailure(String.format("Invalid Connection timeout '%d'.", connectTimeOut),
                           String.format("Ensure the timeout is at least 1 or at most '%d'", MAX_TIMEOUT))
        .withConfigProperty(PROPERTY_SEGEMENT_CONNECTIONTIMEOUT);
    }
  }


  private void validateWriteTimeout(FailureCollector collector) {
    if (containsMacro(PROPERTY_SEGEMENT_WRITETIMEOUT)) {
      return;
    }
    if (writeTimeOut < 1 || writeTimeOut > MAX_TIMEOUT) {
      collector.addFailure(String.format("Invalid Write timeout '%d'.", writeTimeOut),
                           String.format("Ensure the timeout is at least 1 or at most '%d'", MAX_TIMEOUT))
        .withConfigProperty(PROPERTY_SEGEMENT_WRITETIMEOUT);
    }
  }


  private void validateReadTimeout(FailureCollector collector) {
    if (containsMacro(PROPERTY_SEGEMENT_READTIMEOUT)) {
      return;
    }
    if (readTimeOut < 1 || readTimeOut > MAX_TIMEOUT) {
      collector.addFailure(String.format("Invalid Read timeout '%d'.", readTimeOut),
                           String.format("Ensure the timeout is at least 1 or at most '%d'", MAX_TIMEOUT))
        .withConfigProperty(PROPERTY_SEGEMENT_READTIMEOUT);
    }
  }


  /**
   * Utilty class to parse the keyvalue string from UI Widget and return back HashMap.
   * String is of format  <key><keyValueDelimiter><value><delimiter><key><keyValueDelimiter><value>
   * eg:  networktag1=out2internet;networktag2=priority
   * The return from the method is a map with key value pairs of (networktag1 out2internet) and (networktag2 priority)
   *
   * @param configValue       String to be parsed into key values format
   * @param delimiter         Delimiter used for keyvalue pairs
   * @param keyValueDelimiter Delimiter between key and value.
   * @return Map of Key value pairs parsed from input configValue using the delimiters.
   */
  public static Map<String, String> parseKeyValueConfig(@Nullable String configValue, String delimiter,
                                                        String keyValueDelimiter) throws IllegalArgumentException {
    Map<String, String> map = new HashMap<>();
    if (configValue == null) {
      return map;
    }
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter, 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid KeyValue " + property);
      }
      String key = parts[0];
      String value = parts[1];
      map.put(key, value);
    }
    return map;
  }


}
