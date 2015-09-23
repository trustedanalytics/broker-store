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
package org.trustedanalytics.cfbroker.store.hdfs.service;


import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;


/**
 * This test assume building classpath in following order : The test-classes directory, The classes
 * directory, The project dependencies, Additional classpath elements
 *
 * Maven documentation :
 * http://maven.apache.org/surefire/maven-surefire-plugin/examples/configuring-classpath.html
 *
 */
public class ChrootedHdfsClientTest {

    private ChrootedHdfsClient hdfs;

    private DistributedFileSystem fs;

    private static MiniDFSCluster cluster;

    private static final String rootDir = "/junit";

    @BeforeClass
    public static void initialize() throws IOException {
        File baseDir = new File("./target/hdfs/" + "testName").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        Configuration conf = new Configuration(false);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        cluster = builder.build();
        cluster.waitClusterUp();
    }

    @Before
    public void setup() throws IOException {
        fs = cluster.getFileSystem();
        fs.mkdirs(new Path(rootDir));
        hdfs = new ChrootedHdfsClient(fs, rootDir);
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(new Path(rootDir), true);
        fs.close();
    }

    @AfterClass
    public static void shutdown() throws IOException {
        if (cluster != null)
            MiniDFSCluster.shutdownCluster(cluster);
    }

    @Test
    public void testConstructor_success_rootDirCreated() throws Exception {
        assertTrue("Root dir path : " + rootDir + ", was not created during constuctor call",
                fs.exists(new Path(rootDir)));
        assertTrue("Path created during constructor call is not a directory",
                fs.isDirectory(new Path(rootDir)));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testGetNormalizedRootDir_invalidRootDir_exceptionThrown() throws Exception {
        hdfs.getNormalizedRootDir("rootDir");
    }

    @Test
    public void testGetNormalizedRootDir_pathEndsWithSlash_returnsRootDir() throws Exception {
        assertThat(hdfs.getNormalizedRootDir("/rootDir/"), equalTo("/rootDir"));
    }

    @Test
    public void testGetNormalizedRootDir_pathNotEndsWithSlash_returnsRootDir() throws Exception {
        assertThat(hdfs.getNormalizedRootDir("/rootDir"), equalTo("/rootDir"));
    }

    @Test
    public void testCreateEmptyFile_success_emptyFileCreated() throws Exception {
        hdfs.createEmptyFile("/testFile");
        Path filePath = new Path(rootDir + "/testFile");
        assertTrue("Empty file was not created", fs.exists(filePath));
        assertTrue("Created path is not a file", fs.isFile(filePath));

    }

    @Test
    public void testCreateDir_success_dirCreated() throws Exception {
        hdfs.createDir("/testDir");
        Path dirPath = new Path(rootDir + "/testDir");
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
    }

    @Test
    public void testCreateDir_complexPath_dirCreated() throws Exception {
        hdfs.createDir("/path/testDir");
        Path dirPath = new Path(rootDir + "/path/testDir");
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
    }

    @Test
    public void testGetChrootedPath_pathStartsWithSlash_returnsChrootedPath() throws Exception {
        assertThat(hdfs.getChrootedPath("/testPath"), equalTo(new Path(rootDir + "/testPath")));
    }

    @Test
    public void testGetChrootedPath_pathWithoutSlash_returnsChrootedPath() throws Exception {
        assertThat(hdfs.getChrootedPath("testPath"), equalTo(new Path(rootDir + "/" + "testPath")));
    }

    @Test
    public void testAddPathAttr_properlyNamedAttribute_addsAttribute() throws Exception {
        hdfs.createDir("/testDir");
        hdfs.addPathAttr("/testDir", "user.test", "junit".getBytes());
        assertThat(fs.getXAttr(hdfs.getChrootedPath("/testDir"), "user.test"),
                equalTo("junit".getBytes()));
    }

    @Test
    public void testAddPathAttr_complexPath_addsAttribute() throws Exception {
        hdfs.createDir("/testDir/second");
        hdfs.addPathAttr("/testDir/second", "user.test", "junit".getBytes());
        assertThat(fs.getXAttr(hdfs.getChrootedPath("/testDir/second"), "user.test"),
                equalTo("junit".getBytes()));
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testAddPathAttr_attributeWhithoutNamespace_throwsException() throws Exception {
        hdfs.createDir("/testDir");
        hdfs.addPathAttr("/testDir", "test", "junit".getBytes());
    }

    @Test
    public void testGetPathAttr_existingAttribute_returnsValue() throws Exception {
        hdfs.createDir("/testDir");
        fs.setXAttr(hdfs.getChrootedPath("/testDir"), "user.test", "junit".getBytes());
        assertThat(hdfs.getPathAttr("/testDir", "user.test").get(), equalTo("junit".getBytes()));
    }

    @Test
    public void testGetPathAttr_complexExistingPath_returnsValue() throws Exception {
        hdfs.createDir("/testDir/abc");
        fs.setXAttr(hdfs.getChrootedPath("/testDir/abc"), "user.test", "junit".getBytes());
        assertThat(hdfs.getPathAttr("/testDir/abc", "user.test").get(), equalTo("junit".getBytes()));
    }

    @Test
    public void testGetPathAttr_nonExistingAttribute_returnsAbsent() throws Exception {
        hdfs.createDir("/testDir");
        fs.setXAttr(hdfs.getChrootedPath("/testDir"), "user.test", "junit".getBytes());
        assertFalse(hdfs.getPathAttr("/testDir", "user.unknown").isPresent());
    }

    @Test
    public void testGetPathAttr_nonExistingPath_returnsAbsent() throws Exception {
        assertFalse(hdfs.getPathAttr("/nonexistent", "user.unknown").isPresent());
    }


    @Test
    public void testDeleteById_existingPath_deletesPath() throws Exception {
        fs.mkdirs(hdfs.getChrootedPath("/testDir"));
        hdfs.deleteById("testDir");
        assertFalse(fs.exists(hdfs.getChrootedPath("/testDir")));
    }

    @Test
    public void testDeleteById_existingPathWithChild_deletesPath() throws Exception {
        fs.mkdirs(hdfs.getChrootedPath("/testDir"));
        fs.mkdirs(hdfs.getChrootedPath("/testDir/child"));
        hdfs.deleteById("testDir");
        assertFalse(fs.exists(hdfs.getChrootedPath("/testDir")));
    }

    @Test
    public void testDeleteById_existingComplexPath_deletesPath() throws Exception {
        fs.mkdirs(hdfs.getChrootedPath("/testDir/abc"));
        hdfs.deleteById("testDir/abc");
        assertFalse(fs.exists(hdfs.getChrootedPath("/testDir/abc")));
    }

    @Test(expected = IOException.class)
    public void testDeleteById_nonExistingPath_throwsException() throws Exception {
        hdfs.deleteById("nonexistend");
    }

    @Test
    public void testGetDirectSubPathsAttrs_pathIsDirectoryWithSubdirs_returnsList()
            throws Exception {
        fs.mkdirs(hdfs.getChrootedPath("/dirwithdirs/1"));
        fs.mkdirs(hdfs.getChrootedPath("/dirwithdirs/2"));
        fs.setXAttr(hdfs.getChrootedPath("dirwithdirs/1"), "user.attr", "junit1".getBytes());
        fs.setXAttr(hdfs.getChrootedPath("dirwithdirs/2"), "user.attr", "junit2".getBytes());

        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("/dirwithdirs", "user.attr");
        assertThat(attrs.size(), equalTo(2));
        assertThat(attrs, containsInAnyOrder("junit1".getBytes(), "junit2".getBytes()));
    }

    @Test
    public void testGetDirectSubPathsAttrs_pathIsDirectoryWithFiles_returnsList() throws Exception {
        fs.mkdirs(hdfs.getChrootedPath("/dirwithfiles"));
        fs.createNewFile(hdfs.getChrootedPath("/dirwithfiles/1"));
        fs.createNewFile(hdfs.getChrootedPath("/dirwithfiles/2"));

        fs.setXAttr(hdfs.getChrootedPath("/dirwithfiles/1"), "user.attr", "junit1".getBytes());
        fs.setXAttr(hdfs.getChrootedPath("/dirwithfiles/2"), "user.attr", "junit2".getBytes());

        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("/dirwithfiles", "user.attr");
        assertThat(attrs.size(), equalTo(2));
        assertThat(attrs, containsInAnyOrder("junit1".getBytes(), "junit2".getBytes()));
    }

    @Test
    public void testGetDirectSubPathsAttrs_pathIsEmptyDirectory_returnsEmptyList() throws Exception {
        fs.mkdirs(hdfs.getChrootedPath("empty"));
        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("empty", "user.nonexistent");
        assertTrue(attrs.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDirectSubPathsAttrs_pathIsNotDirectory_throwsException() throws Exception {
        fs.createNewFile(hdfs.getChrootedPath("newFile"));
        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("newFile", "user.nonexistent");
        assertTrue(attrs.isEmpty());
    }
}
