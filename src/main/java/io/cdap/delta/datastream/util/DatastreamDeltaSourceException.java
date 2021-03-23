/*
 *
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.delta.datastream.util;

import com.google.api.core.ApiFuture;
import com.google.cloud.datastream.v1alpha1.OperationMetadata;

/**
 * Exception thrown by Datastream Delta Source on purpose. Such exception is thrown after Datastream Delta Source
 * detects certain errors. Such exception should be handled by restarting the Datastream Delta Source. For example,
 * some long running operation of datastream failed (create stream , start stream , pause stream, etc.)
 * This exception is used to differentiate from those exception:
 * 1) Some runtime exception that was not caught by Datastream Delta Source (for example, NPE)
 * 2) Some exception that could not be recovered. (for example, datastream generated a result dump/cdc file that
 * cannot be parsed.)
 */
public class DatastreamDeltaSourceException extends RuntimeException {
  private final ApiFuture<OperationMetadata> metadata;

  public DatastreamDeltaSourceException(String errorMessage, Exception cause) {
    this(errorMessage, cause, null);
  }
  public DatastreamDeltaSourceException(String errorMessage, Exception cause, ApiFuture<OperationMetadata> metadata) {
    super(errorMessage, cause);
    this.metadata = metadata;
  }
  public DatastreamDeltaSourceException(String errorMessage) {
    super(errorMessage);
    this.metadata = null;
  }
  public ApiFuture<OperationMetadata> getMetadata() {
    return metadata;
  }
}
