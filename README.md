[![Dependency Status](https://www.versioneye.com/user/projects/5723603aba37ce00350af438/badge.svg?style=flat)](https://www.versioneye.com/user/projects/5723603aba37ce00350af438)

# Cloud Foundry broker metadata repository

Library with storage utils for Cloudfoundry broker development.

# Functionality

This library allows you to store objects in key/value store. It contains 2 implementations: hdfs-store and zookeeper-store.

# How to use it?

To use this library, you need to build it from sources and install it in your local maven repository. Then you can add dependency to application's ```pom.xml``` and use it in code. Follow steps described below.

## Build

Run below maven command. It will:
* compile code
* run unit&integration tests
* create jar package 
* install package to your local maven repository

```mvn clean install```

## Adding dependencies

###### broker-commons
It's library containing common classes like interfaces or serializers/deserializers. You should add below code to your ```pom.xml``` no matter if you'd like to use hdfs-store or zookeeper-store.
```
<dependency>
  <groupId>org.trustedanalytics.servicebroker.repository</groupId>
  <artifactId>broker-store-commons</artifactId>
  <version><!-- insert latest version number here --></version>
</dependency>
```

###### hdfs-store
Add below section to your ```pom.xml``` if you'd like to use hdfs-store.
```
<dependency>
  <groupId>org.trustedanalytics.servicebroker.repository</groupId>
  <artifactId>hdfs-broker-store</artifactId>
  <version><!-- insert latest version number here --></version>
</dependency>
```

###### zookeeper-store
Add below section to your ```pom.xml``` if you'd like to use zookeeper-store.
```
<dependency>
  <groupId>org.trustedanalytics.servicebroker.repository</groupId>
  <artifactId>zookeeper-broker-store</artifactId>
  <version><!-- insert latest version number here --></version>
</dependency>
```

## Use in code

#### hdfs-store

#### zookeeper-store
Create and initialize Zookeeper client. Use ZookeeperClientBuilder. Pass parameters in constructor:
* Connection string - comma delimited host:port pairs
* Username (user who has access to root directory)
* Password (to root directory)
* Root directory (will be used as root for all your operations)

You can also invoke ```withRetryPolicy()``` method on builder to provide custom policy (pass object of RetryPolicy class from org.apache.curator package).

Example:
```
ZookeeperClient zookeeperClient =
    new ZookeeperClientBuilder("127.0.0.1:2181", "admin", "haselko_maselko", "/test3").build();
zookeeperClient.init();
```
After finishing job with zookeeper you should invoke ```destroy``` method. If your are using Spring you can use Bean properties. Spring will invoke ```init``` and ```destroy``` methods for you. Example below:
```
@Bean(initMethod = "init", destroyMethod = "destroy")
protected ZookeeperClient zookeeperClient() {
    return new BasicZookeeperClient("127.0.0.1:2181", "admin", "haselko_maselko", "/cf/test");
}
```

Then you can create Broker store. You should pass following parameters:
* Zookeeper client created in the step above
* Serializer (you can use default JSON serializer available in broker-store-commons or implement your own)
* Deserializer (you can use default JSON deserializer available in broker-store-commons or implement your own)

Below you can see example of BrokerStore for objects of class MyClass using default JSON (de)serailizer.
```
BrokerStore<MyClass> store = new ZookeeperStore<>(zookeeperClient,
    JSONSerDeFactory.getInstance().getSerializer(),
    JSONSerDeFactory.getInstance().getDeserializer(MyClass.class));
```

### zip configurations

HadoopZipConfiguration can be used in broker to obtain hadoop configuration or credentials based on encoded zip file.

Examples:

Get as hadoop configuration:
```
Configuration hadoopConfiguration =
        HadoopZipConfiguration.createHadoopZipConfiguration(encodedZip).getAsHadoopConfiguration();
```
Get as key-value map:
```
Map<String, String> map = HadoopZipConfiguration.createHadoopZipConfiguration(encodedZip).getAsParameterMap()
```

Get broker credentials:
```
ImmutableMap credentials =
        HadoopZipConfiguration.createHadoopZipConfiguration(encodedZip).getBrokerCredentials();
```
