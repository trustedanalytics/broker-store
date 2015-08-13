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

import com.google.common.base.Throwables;
import org.trustedanalytics.cfbroker.store.helper.FunctionThatThrows;

import java.io.IOException;
import java.util.function.Consumer;

public class CuratorExceptionHandler {

    private CuratorExceptionHandler() {
    }

    public static <R> R propagateAsIOException(FunctionThatThrows<R> function,
        Consumer<String> logging, String message) throws IOException {

        try {
            return function.apply();
        } catch (Exception e) {
            logging.accept(message);
            Throwables.propagateIfInstanceOf(e, IOException.class);
            Throwables.propagateIfInstanceOf(e, RuntimeException.class);
            throw new IOException(e);
        }
    }
}
