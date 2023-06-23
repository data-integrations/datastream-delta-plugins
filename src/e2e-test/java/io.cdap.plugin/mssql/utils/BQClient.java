/*
 * Copyright (c) 2023.
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

package io.cdap.plugin.mssql.utils;

import io.cdap.e2e.utils.PluginPropertyUtils;

import java.util.concurrent.TimeUnit;

/**
 * Contains bq helper methods used in e2e tests.
 */
public class BQClient {

    public static void waitForFlush() throws InterruptedException {
        int flushInterval = Integer.parseInt(PluginPropertyUtils.pluginProp("loadInterval"));
        TimeUnit time = TimeUnit.SECONDS;
        time.sleep(2 * flushInterval + 60);
    }
}
