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
package org.trustedanalytics.cfbroker.catalog;

public class ServicePlan {

    private String id;
    private String name;
    private String description;
    private Boolean free;

    public ServicePlan() {
    }

    public ServicePlan(String id, String name, String description, Boolean free) {
        this.id = id;
        this.name = name;
        this.description = description;
        this.free = free;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Boolean isFree() {
        return free;
    }

    public void setFree(Boolean free) {
        this.free = free;
    }

    public void validate() {
        if (id == null || name == null || description == null || free == null) {
            throw new IllegalArgumentException("Service plan should contain such not-null fields: "
                + "id:String, name:String, description:String, free:Boolean. "
                + String.format("Provided id:'%s', name:'%s', description:'%s', free:'%s'.",
                                                        id, name, description, free));
        }
    }
}
