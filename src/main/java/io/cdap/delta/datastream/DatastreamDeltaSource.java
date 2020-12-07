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

package io.cdap.delta.datastream;

import com.google.cloud.ServiceOptions;
import com.google.cloud.datastream.v1alpha1.DatastreamClient;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.delta.api.Configurer;
import io.cdap.delta.api.DeltaSource;
import io.cdap.delta.api.DeltaSourceContext;
import io.cdap.delta.api.EventEmitter;
import io.cdap.delta.api.EventReaderDefinition;
import io.cdap.delta.api.SourceConfigurer;
import io.cdap.delta.api.SourceProperties;
import io.cdap.delta.api.assessment.TableAssessor;
import io.cdap.delta.api.assessment.TableDetail;
import io.cdap.delta.api.assessment.TableRegistry;

import java.io.IOException;

/**
 * Datastream origin.
 */
@Plugin(type = DeltaSource.PLUGIN_TYPE)
@Name(DatastreamDeltaSource.NAME)
@Description("Delta source for Datastream.")
public class DatastreamDeltaSource implements DeltaSource {

  public static final String NAME = "datastream";

  private final DatastreamConfig conf;

  public DatastreamDeltaSource(DatastreamConfig conf) {
    conf.validate();
    this.conf = conf;
  }

  @Override
  public void configure(SourceConfigurer configurer) {
    configurer.setProperties(new SourceProperties.Builder().setRowIdSupported(true).build());
  }

  @Override
  public DatastreamEventReader createReader(EventReaderDefinition definition, DeltaSourceContext context,
                                  EventEmitter eventEmitter) {
    Storage storage = StorageOptions.newBuilder()
      .setCredentials(conf.getGcsCredentials())
      .setProjectId(ServiceOptions.getDefaultProjectId())
      .build()
      .getService();

    try {
      return new DatastreamEventReader(conf, definition, context, eventEmitter, DatastreamClient.create(), storage);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Datastream client!", e);
    }
  }

  @Override
  public TableRegistry createTableRegistry(Configurer configurer) throws Exception {
      return new DatastreamTableRegistry(conf, DatastreamClient.create());
  }

  @Override
  public TableAssessor<TableDetail> createTableAssessor(Configurer configurer) throws Exception {
    return new DatastreamTableAssessor(conf);
  }
}
