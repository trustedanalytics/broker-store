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
package org.trustedanalytics.cfbroker.store.api;

import java.io.IOException;
import java.util.Optional;

/**
 * Classes implementing that interface should be able to reliably store Object of type T, identified by
 * String id. It was intended to be used with Cloud Foundry brokers. Possible implementations includes : hdfs,
 * zookeeper, hbase.
 */
public interface BrokerStore<T> {

    Optional<T> getById(Location location) throws IOException;

    void save(Location location, T t) throws IOException;

    Optional<T> deleteById(Location location) throws IOException;
}
