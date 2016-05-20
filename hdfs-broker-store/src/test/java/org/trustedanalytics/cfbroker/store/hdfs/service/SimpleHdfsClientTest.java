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


import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;


/**
 * This test assume building classpath in following order : The test-classes directory, The classes
 * directory, The project dependencies, Additional classpath elements
 *
 * Maven documentation :
 * http://maven.apache.org/surefire/maven-surefire-plugin/examples/configuring-classpath.html
 *
 */
public class SimpleHdfsClientTest {

    private static final FsPermission fsPermission = new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE);

    private static MiniDFSCluster cluster;

    private SimpleHdfsClient hdfs;

    private DistributedFileSystem fs;

    @BeforeClass
    public static void initialize() throws IOException {
        File baseDir = new File("./target/hdfs/" + "testName").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        Configuration conf = new Configuration(false);
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        conf.setBoolean(DFSConfigKeys.DFS_PERMISSIONS_ENABLED_KEY, true);
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        cluster = builder.build();
        cluster.waitClusterUp();
    }

    @Before
    public void setup() throws IOException {
        fs = cluster.getFileSystem();
        hdfs = new SimpleHdfsClient(fs);
    }

    @After
    public void tearDown() throws IOException {
        fs.delete(new Path("/"), true);
        fs.close();
    }

    @AfterClass
    public static void shutdown() throws IOException {
        if (cluster != null)
            MiniDFSCluster.shutdownCluster(cluster);
    }

    @Test
    public void testCreateEmptyFile_relativePath_emptyFileCreated() throws Exception {
        hdfs.createEmptyFile("testFile");
        Path filePath = new Path("/testFile");
        assertTrue("Empty file was not created", fs.exists(filePath));
        assertTrue("Created path is not a file", fs.isFile(filePath));

    }

    @Test
    public void testCreateDir_absolutePath_dirCreated() throws Exception {
        hdfs.createDir("/testDir");
        Path dirPath = new Path("/testDir");
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
    }

    @Test
    public void testCreateDir_alreadyExsits_throwIOException() throws Exception {
        hdfs.createDir("/testDir");
        hdfs.createDir("/testDir");
    }

    @Test
    public void testCreateDirWithPermissions_absolutePath_dirCreated() throws Exception {
        Path dirPath = new Path("/testDirPermission");
        hdfs.createDir("/testDirPermission", fsPermission);
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
        assertTrue("Wrong permissions for created directory", fs.getFileStatus(dirPath)
            .getPermission().equals(fsPermission));
    }

    @Test
    public void testCreateDirWithPermissions_alreadyExsits_throwIOException() throws Exception {
        hdfs.createDir("/testDir", fsPermission);
        hdfs.createDir("/testDir", fsPermission);
    }

    @Test
    public void testCreateEmptyFile_absolutePath_emptyFileCreated() throws Exception {
        hdfs.createEmptyFile("/testFile");
        Path filePath = new Path("/testFile");
        assertTrue("Empty file was not created", fs.exists(filePath));
        assertTrue("Created path is not a file", fs.isFile(filePath));
    }

    @Test
    public void testCreateDir_complexPath_dirCreated() throws Exception {
        hdfs.createDir("path/testDir");
        Path dirPath = new Path("/path/testDir");
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
    }

    @Test
    public void testAddPathAttr_properlyNamedAttribute_addsAttribute() throws Exception {
        hdfs.createDir("testDir");
        hdfs.addPathAttr("/testDir", "user.test", "junit".getBytes());
        assertThat(fs.getXAttr(new Path("/testDir"), "user.test"),
                equalTo("junit".getBytes()));
    }

    @Test
    public void testAddPathAttr_complexPath_addsAttribute() throws Exception {
        hdfs.createDir("testDir/second");
        hdfs.addPathAttr("/testDir/second", "user.test", "junit".getBytes());
        assertThat(fs.getXAttr(new Path("/testDir/second"), "user.test"),
                equalTo("junit".getBytes()));
    }

    @Test(expected = HadoopIllegalArgumentException.class)
    public void testAddPathAttr_attributeWhithoutNamespace_throwsException() throws Exception {
        hdfs.createDir("testDir");
        hdfs.addPathAttr("/testDir", "test", "junit".getBytes());
    }

    @Test
    public void testGetPathAttr_existingAttribute_returnsValue() throws Exception {
        hdfs.createDir("testDir");
        fs.setXAttr(new Path("/testDir"), "user.test", "junit".getBytes());
        assertThat(hdfs.getPathAttr("/testDir", "user.test").get(), equalTo("junit".getBytes()));
    }

    @Test
    public void testGetPathAttr_complexExistingPath_returnsValue() throws Exception {
        hdfs.createDir("testDir/abc");
        fs.setXAttr(new Path("/testDir/abc"), "user.test", "junit".getBytes());
        assertThat(hdfs.getPathAttr("/testDir/abc", "user.test").get(), equalTo("junit".getBytes()));
    }

    @Test
    public void testGetPathAttr_nonExistingAttribute_returnsAbsent() throws Exception {
        hdfs.createDir("testDir");
        fs.setXAttr(new Path("/testDir"), "user.test", "junit".getBytes());
        assertFalse(hdfs.getPathAttr("/testDir", "user.unknown").isPresent());
    }

    @Test
    public void testGetPathAttr_nonExistingPath_returnsAbsent() throws Exception {
        assertFalse(hdfs.getPathAttr("/nonexistent", "user.unknown").isPresent());
    }

    @Test
    public void testDeleteById_existingPath_deletesPath() throws Exception {
        fs.mkdirs(new Path("/testDir"));
        hdfs.deleteById("testDir");
        assertFalse(fs.exists(new Path("/testDir")));
    }


    @Test
    public void testDeleteById_existingPathWithChild_deletesPath() throws Exception {
        String path = "testDir";
        hdfs.createDir(path);
        fs.mkdirs(new Path("/testDir/child"));
        hdfs.deleteById(path);
        assertFalse(fs.exists(new Path(path)));
    }

    @Test
    public void testDeleteById_existingComplexPath_deletesPath() throws Exception {
        String path = "testDir/abc";
        hdfs.createDir(path);
        hdfs.deleteById(path);
        assertFalse(fs.exists(new Path("/testDir/abc")));
    }

    @Test(expected = IOException.class)
    public void testDeleteById_nonExistingPath_throwsException() throws Exception {
        hdfs.deleteById("nonexistend");
    }

    @Test
    public void testGetDirectSubPathsAttrs_pathIsDirectoryWithSubdirs_returnsList()
            throws Exception {
        fs.mkdirs(new Path("/dirwithdirs/1"));
        fs.mkdirs(new Path("/dirwithdirs/2"));
        fs.setXAttr(new Path("/dirwithdirs/1"), "user.attr", "junit1".getBytes());
        fs.setXAttr(new Path("/dirwithdirs/2"), "user.attr", "junit2".getBytes());

        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("/dirwithdirs", "user.attr");
        assertThat(attrs.size(), equalTo(2));
        assertThat(attrs, containsInAnyOrder("junit1".getBytes(), "junit2".getBytes()));
    }

    @Test
    public void testGetDirectSubPathsAttrs_pathIsDirectoryWithFiles_returnsList() throws Exception {
        fs.mkdirs(new Path("/dirwithfiles"));
        fs.createNewFile(new Path("/dirwithfiles/1"));
        fs.createNewFile(new Path("/dirwithfiles/2"));

        fs.setXAttr(new Path("/dirwithfiles/1"), "user.attr", "junit1".getBytes());
        fs.setXAttr(new Path("/dirwithfiles/2"), "user.attr", "junit2".getBytes());

        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("/dirwithfiles", "user.attr");
        assertThat(attrs.size(), equalTo(2));
        assertThat(attrs, containsInAnyOrder("junit1".getBytes(), "junit2".getBytes()));
    }

    @Test
    public void testGetDirectSubPathsAttrs_pathIsEmptyDirectory_returnsEmptyList() throws Exception {
        fs.mkdirs(new Path("/empty"));
        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("empty", "user.nonexistent");
        assertTrue(attrs.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetDirectSubPathsAttrs_pathIsNotDirectory_throwsException() throws Exception {
        fs.createNewFile(new Path("/newFile"));
        List<byte[]> attrs = hdfs.getDirectSubPathsAttrs("newFile", "user.nonexistent");
        assertTrue(attrs.isEmpty());
    }

    @Test
    public void testSetPermission_directoryCreated_PermissionsChanged() throws Exception {
        hdfs.createDir("/testDir");
        Path dirPath = new Path("/testDir");
        hdfs.setPermission("/testDir", fsPermission);
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
        assertTrue("Wrong permissions for created directory", fs.getFileStatus(dirPath)
            .getPermission().equals(fsPermission));
    }

    @Test(expected = IOException.class)
    public void testSetPermission_directoryNotExsits_throwsIOException() throws Exception {
        Path dirPath = new Path("/testDir");
        hdfs.setPermission("/testDir", fsPermission);
    }

    @Test
    public void testSetOwner_directoryCreated_OwnerChanged() throws Exception {
        hdfs.createDir("/testDir");
        Path dirPath = new Path("/testDir");
        hdfs.setOwner("/testDir", "hdfs", "supergroup");
        assertTrue("Dir was not created", fs.exists(dirPath));
        assertTrue("Created path is not a directory", fs.isDirectory(dirPath));
        assertTrue("Wrong permissions for created directory", fs.getFileStatus(dirPath).getOwner()
            .equals("hdfs"));
    }

    @Test(expected = IOException.class)
    public void testSetOwner_directoryNotExsits_throwsIOException() throws Exception {
        Path dirPath = new Path("/testDir");
        hdfs.setOwner("/testDir", "hdfs", "supergroup");
    }
}
