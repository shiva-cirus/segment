/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.common;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates Operation Type which will be used for Segment API
 */
public enum SegmentOperationType {

  /**
   * Identify
   */
  IDENTIFY("Identify"),

  /**
   * Track
   */
  TRACK("Track"),

  /**
   * Screen
   */
  SCREEN("Screen"),

  /**
   * Page
   */
  PAGE("Page"),

  /**
   * Group
   */
  GROUP("Group"),

  /**
   * Alias
   */
  ALIAS("Alias");

  private final String value;

  SegmentOperationType(String value) {
    this.value = value;
  }

  /**
   * Converts id type string value into {@link SegmentOperationType} enum.
   *
   * @param stringValue id type string value
   * @return sink id type in optional container
   */
  public static Optional<SegmentOperationType> fromValue(String stringValue) {
    return Stream.of(values())
      .filter(keyType -> keyType.value.equalsIgnoreCase(stringValue))
      .findAny();
  }

  public static String getSupportedTypes() {
    return Arrays.stream(SegmentOperationType.values()).map(SegmentOperationType::getValue).collect(Collectors.joining(", "));
  }

  public String getValue() {
    return value;
  }
}
