/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.trino;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;


import java.util.Map;

/** Trino {@link ConnectorFactory}. */
public abstract class TrinoConnectorFactoryBase implements ConnectorFactory {
    @Override
    public String getName() {
        return "tablestore";
    }

    @Override
    public Connector create(
            String catalogName, Map<String, String> config, ConnectorContext context) {
        Configuration configuration = Configuration.fromMap(config);
        // initialize file system
        FileSystem.initialize(
                configuration, PluginUtils.createPluginManagerFromRootFolder(configuration));
        try {
            SecurityUtils.install(new SecurityConfiguration(configuration));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new TrinoConnector(
                new TrinoMetadata(configuration),
                new TrinoSplitManager(),
                new TrinoPageSourceProvider());
    }
}
