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
package org.trustedanalytics.cfbroker.config;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class HadoopZipConfigurationTest {

  private static String ENCODED_ZIP_FILE = "/encodedZip.txt";

  private static String encodedZip;

  @Before
  public void setUp() {
    try (InputStream hl = getClass().getResourceAsStream(ENCODED_ZIP_FILE)) {
      encodedZip = IOUtils.toString(hl);
    } catch (IOException ignored) {
    }
  }

  @Test
  public void getAsHadoopConfiguration_correctEncodedZip_returnHadoopConfiguration()
      throws Exception {
    Configuration hadoopConfiguration =
        HadoopZipConfiguration.createHadoopZipConfiguration(encodedZip).getAsHadoopConfiguration();
    assertThat(hadoopConfiguration.get("hadoop.security.authentication"), equalTo("kerberos"));
    assertThat(hadoopConfiguration.get("dfs.namenode.kerberos.principal"),
        equalTo("hdfs/_HOST@US-WEST-2.COMPUTE.INTERNAL"));
  }

  @Test
  public void getAsMap_correctEncodedZip_returnMapOfHadoopProperties() throws Exception {
    Map<String, String> map =
        HadoopZipConfiguration.createHadoopZipConfiguration(encodedZip).getAsParameterMap();
    assertThat(map.get("hadoop.security.authentication"), equalTo("kerberos"));
    assertThat(map.get("dfs.namenode.kerberos.principal"),
        equalTo("hdfs/_HOST@US-WEST-2.COMPUTE.INTERNAL"));
  }

  @Test
  public void getAsMap_emptyBase64OfZip_returnEmptyMap() throws Exception {
    Map<String, String> map = HadoopZipConfiguration.createHadoopZipConfiguration("").getAsParameterMap();
    assertThat(map.entrySet().size(), equalTo(0));
  }

  @Test
  public void getAsHadoopConfiguration_emptyBase64OfZip_returnEmptyConfiguration() throws Exception {
    Configuration hadoopConfiguration =
        HadoopZipConfiguration.createHadoopZipConfiguration("").getAsHadoopConfiguration();
    assertThat(hadoopConfiguration.get("hadoop.security.authentication"), equalTo("simple"));
  }

  @Test
  public void getAsBrokerCredentialsFromZip_correctEncodedZip_returnBrokerCredentials()
      throws Exception {
    ImmutableMap credentials =
        HadoopZipConfiguration.createHadoopZipConfiguration(encodedZip).getBrokerCredentials();
    ImmutableMap zipValue = (ImmutableMap) credentials.get(ConfigConstants.HADOOP_CONFIG_ZIP_VALUE);
    ImmutableMap jsonValue =
        (ImmutableMap) credentials.get(ConfigConstants.HADOOP_CONFIG_KEY_VALUE);

    assertThat(jsonValue.get("hadoop.security.authentication"), equalTo("kerberos"));
    assertThat(jsonValue.get("dfs.namenode.kerberos.principal"),
        equalTo("hdfs/_HOST@US-WEST-2.COMPUTE.INTERNAL"));
    assertThat(zipValue.containsKey(ConfigConstants.DESCRIPTION_KEY), equalTo(true));
    assertThat(zipValue.get(ConfigConstants.DESCRIPTION_KEY), equalTo(ConfigConstants.DESCRIPTION));
    assertThat(zipValue.containsKey(ConfigConstants.ZIP), equalTo(true));
    assertThat(zipValue.get(ConfigConstants.ZIP), equalTo(encodedZip));
  }

  @Test
  public void getAsBrokerCredentialsFromZip_incorrectEncodedZip_returnBrokerCredentials()
      throws Exception {
    ImmutableMap credentials =
        HadoopZipConfiguration.createHadoopZipConfiguration("").getBrokerCredentials();
    ImmutableMap zipValue = (ImmutableMap) credentials.get(ConfigConstants.HADOOP_CONFIG_ZIP_VALUE);
    ImmutableMap jsonValue =
        (ImmutableMap) credentials.get(ConfigConstants.HADOOP_CONFIG_KEY_VALUE);

    assertThat(jsonValue.isEmpty(), equalTo(true));
    assertThat(zipValue.containsKey(ConfigConstants.DESCRIPTION_KEY), equalTo(true));
    assertThat(zipValue.get(ConfigConstants.DESCRIPTION_KEY), equalTo(ConfigConstants.DESCRIPTION));
    assertThat(zipValue.containsKey(ConfigConstants.ZIP), equalTo(true));
    assertThat(zipValue.get(ConfigConstants.ZIP), equalTo(""));
  }
}
