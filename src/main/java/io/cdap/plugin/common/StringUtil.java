package io.cdap.plugin.common;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class StringUtil {
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
