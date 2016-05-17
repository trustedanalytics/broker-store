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
package org.trustedanalytics.cfbroker.store.hdfs.helper;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.io.File;
import java.util.List;

public class DirHelper {

    private DirHelper() {
    }

    public static String removeTrailingSlashes(String path) {
        return path.replaceFirst("/*$", "");
    }

    public static String removeLeadingSlashes(String path) {
        return path.replaceFirst("^/*", "");
    }

    public static String addLeadingSlash(String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    public static List<String> dirList(String path){
        Builder<String> builder = ImmutableList.builder();
        File folder = new File(path);
        File[] files = folder.listFiles();
        if(files == null) {
            throw new NullPointerException();
        }
        for (File entry : files) {
            if (entry.isFile()) {
                builder.add("File : " + entry.getName());
            } else if (entry.isDirectory()) {
                builder.add("Directory : " + entry.getName());
            }
        }
        return builder.build();
    }

    public static String concat(String path1, String path2) {
        if(Strings.isNullOrEmpty(path2)) {
            return path1;
        }
        if(Strings.isNullOrEmpty(path1)) {
            return path2;
        }
        return removeTrailingSlashes(path1) + "/" + removeLeadingSlashes(path2);
    }

}
