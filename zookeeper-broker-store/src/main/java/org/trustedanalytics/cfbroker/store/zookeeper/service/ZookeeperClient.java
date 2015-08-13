/**
 * Copyright (c) 2015 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.trustedanalytics.cfbroker.store.zookeeper.service;

import java.io.IOException;
import java.util.List;

public interface ZookeeperClient {

    void init() throws IOException;

    void destroy();

    String getRootDir();

    void addZNode(String path, byte[] zNodeContent) throws IOException;

    byte[] getZNode(String path) throws IOException;

    void deleteZNode(String path) throws IOException;

    List<String> getChildrenNames() throws IOException;
}
