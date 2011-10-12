/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.hadoop;

import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildCommon;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildHdfs;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildMapReduce;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

public abstract class HadoopClusterActionHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopClusterActionHandler.class);
    
  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with a hadoop defaults
   * properties.
   */
  protected Configuration getConfiguration(
      ClusterSpec clusterSpec) throws IOException {
    return getConfiguration(clusterSpec, "whirr-hadoop-default.properties");
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, "hadoop", "install_hadoop");
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, "hadoop", "configure_hadoop");
  }
  
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();

    Configuration conf = getConfiguration(clusterSpec);
    addStatement(event, call("configure_hostnames", "-c", clusterSpec.getProvider()));

    addStatement(event, call("install_java"));
    addStatement(event, call("install_tarball"));

    String tarball = prepareRemoteFileUrl(event,
        conf.getString("whirr.hadoop.tarball.url"));
    
    addStatement(event, call(getInstallFunction(conf),
        "-c", clusterSpec.getProvider(),
        "-u", tarball));
  }
  
  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    doBeforeConfigure(event);

    createHadoopConfigFiles(event, clusterSpec, cluster);
    
    addStatement(event, call(
      getConfigureFunction(getConfiguration(clusterSpec)),
      Joiner.on(",").join(event.getInstanceTemplate().getRoles()),
      "-c", clusterSpec.getProvider())
    );
  }

  protected void doBeforeConfigure(ClusterActionEvent event) throws IOException {};

  private void createHadoopConfigFiles(ClusterActionEvent event,
      ClusterSpec clusterSpec, Cluster cluster) throws IOException {
    try {
      event.getStatementBuilder().addStatements(
        buildCommon("/tmp/core-site.xml", clusterSpec, cluster),
        buildHdfs("/tmp/hdfs-site.xml", clusterSpec, cluster),
        buildMapReduce("/tmp/mapred-site.xml", clusterSpec, cluster)
      );
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    // TODO: wait for TTs to come up (done in test for the moment)
    
    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());

    InetAddress namenode = HadoopCluster.getNamenodePublicAddress(cluster);
    LOG.info("Namenode web UI available at http://{}:{}", namenode, HadoopCluster.NAMENODE_WEB_UI_PORT);
    InetAddress jobtracker = HadoopCluster.getJobTrackerPublicAddress(cluster);
    LOG.info("Jobtracker web UI available at http://{}:{}", jobtracker, HadoopCluster.JOBTRACKER_WEB_UI_PORT);
    
    Properties config = createClientSideProperties(clusterSpec, namenode, jobtracker);
    createClientSideHadoopSiteFile(clusterSpec, config);
    createProxyScript(clusterSpec, cluster);
    event.setCluster(new Cluster(cluster.getInstances(), config));
  }

  private Properties createClientSideProperties(ClusterSpec clusterSpec,
      InetAddress namenode, InetAddress jobtracker) throws IOException {
    Properties config = new Properties();
    config.setProperty("hadoop.job.ugi", "root,root");
    config.setProperty("fs.default.name", String.format("hdfs://%s:8020/", namenode.getHostName()));
    config.setProperty("mapred.job.tracker", String.format("%s:8021", jobtracker.getHostName()));
    config.setProperty("hadoop.socks.server", "localhost:6666");
    config.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
    if (clusterSpec.getProvider().endsWith("ec2")) {
      config.setProperty("fs.s3.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3.awsSecretAccessKey", clusterSpec.getCredential());
      config.setProperty("fs.s3n.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3n.awsSecretAccessKey", clusterSpec.getCredential());
    }
    return config;
  }

  private void createClientSideHadoopSiteFile(ClusterSpec clusterSpec, Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "hadoop-site.xml");
    try {
      Files.write(generateHadoopConfigurationFile(config), hadoopSiteFile,
          Charsets.UTF_8);
      LOG.info("Wrote Hadoop site file {}", hadoopSiteFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop site file {}", hadoopSiteFile, e);
    }
  }
  
  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }
  
  private CharSequence generateHadoopConfigurationFile(Properties config) {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\"?>\n");
    sb.append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n");
    sb.append("<configuration>\n");
    for (Entry<Object, Object> entry : config.entrySet()) {
      sb.append("  <property>\n");
      sb.append("    <name>").append(entry.getKey()).append("</name>\n");
      sb.append("    <value>").append(entry.getValue()).append("</value>\n");
      sb.append("  </property>\n");
    }
    sb.append("</configuration>\n");
    return sb;
  }
  
  private void createProxyScript(ClusterSpec clusterSpec, Cluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopProxyFile = new File(configDir, "hadoop-proxy.sh");
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      InetAddress namenode = HadoopCluster.getNamenodePublicAddress(cluster);
      String script = String.format("echo 'Running proxy to Hadoop cluster at %s. " +
          "Use Ctrl-c to quit.'\n", namenode.getHostName())
          + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, hadoopProxyFile, Charsets.UTF_8);
      LOG.info("Wrote Hadoop proxy script {}", hadoopProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop proxy script {}", hadoopProxyFile, e);
    }
  }

}
