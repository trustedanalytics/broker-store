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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.common.collect.ImmutableMap;

public final class HadoopZipConfiguration implements HadoopConfiguration {

  private final String encodedZip;

  private final Map<String, String> configParams;

  private HadoopZipConfiguration(String encodedZip) throws IOException, ConfigurationException{
    this.encodedZip = encodedZip;
    try {
      this.configParams = getAsMap();
    } catch (XPathExpressionException e) {
      throw new ConfigurationException("Zip configuration corrupted", e);
    }
  }

  public static HadoopZipConfiguration createHadoopZipConfiguration(String encodedZip)
      throws IOException, ConfigurationException {
    return new HadoopZipConfiguration(encodedZip);
  }

  public Configuration getAsHadoopConfiguration() {
    Configuration configuration = new Configuration();
    configParams.entrySet().forEach((pair) -> configuration.set(pair.getKey(), pair.getValue()));
    return configuration;
  }

  public ImmutableMap getBrokerCredentials() {
    return ImmutableMap.of(ConfigConstants.HADOOP_CONFIG_KEY_VALUE, ImmutableMap.copyOf(configParams),
        ConfigConstants.HADOOP_CONFIG_ZIP_VALUE, ImmutableMap.of(ConfigConstants.DESCRIPTION_KEY,
            ConfigConstants.DESCRIPTION, ConfigConstants.ZIP, encodedZip));
  }

  public Map<String, String> getAsParameterMap() {
    return configParams;
  }

  private Map<String, String> getAsMap() throws IOException, XPathExpressionException {
    InputStream zipInputStream =
        new ZipInputStream(new ByteArrayInputStream(Base64.decodeBase64(encodedZip)));
    ZipEntry zipFileEntry;
    Map<String, String> map = new HashMap<>();
    while ((zipFileEntry = ((ZipInputStream) zipInputStream).getNextEntry()) != null) {
      if (!zipFileEntry.getName().endsWith("-site.xml")) {
        continue;
      }
      byte[] bytes = IOUtils.toByteArray(zipInputStream);
      InputSource is = new InputSource(new ByteArrayInputStream(bytes));
      XPath xPath = XPathFactory.newInstance().newXPath();
      NodeList nodeList =
          (NodeList) xPath
              .evaluate(ConfigConstants.CONF_PROPERTY_XPATH, is, XPathConstants.NODESET);
      for (int i = 0; i < nodeList.getLength(); i++) {
        Node propNode = nodeList.item(i);
        String key = (String) xPath.evaluate("name/text()", propNode, XPathConstants.STRING);
        String value = (String) xPath.evaluate("value/text()", propNode, XPathConstants.STRING);
        map.put(key, value);
      }
    }
    return map;
  }

}
