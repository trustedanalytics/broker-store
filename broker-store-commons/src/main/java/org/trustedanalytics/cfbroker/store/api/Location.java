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

import org.trustedanalytics.cfbroker.store.helper.PathHelper;

import java.util.Objects;
import java.util.Optional;

public class Location {

    private final String id;

    private final Optional<String> parentId;

    public Location(String id, Optional<String> parentId) {
        this.id = id;
        this.parentId = parentId;
    }

    public static Location newInstance(String id) {
        return new Location(id, Optional.empty());
    }

    public static Location newInstance(String id, String parentId) {
        return new Location(id, Optional.ofNullable(parentId));
    }

    public Optional<String> getParentId() {
        return parentId;
    }

    public String getId() {
        return id;
    }

    public String getPath() {
        return PathHelper.normalizePath(parentId.orElse("")) + PathHelper.normalizePath(id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        Location other = (Location) o;
        return Objects.equals(id, other.id)
            && Objects.equals(parentId, other.parentId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, parentId);
    }

    @Override
    public String toString() {
        return "Location{" + "id='" + id + '\'' + ", parentId=" + parentId + '}';
    }
}
