/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kyuubi.engine.hive.yarn


import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.ipc.CallerContext
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records.{ApplicationId, ApplicationSubmissionContext, ContainerLaunchContext, LocalResource, Resource}
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.{ENGINE_HIVE_YARN_CORES, ENGINE_HIVE_YARN_MAX_ATTEMPTS, ENGINE_HIVE_YARN_MEMORY, ENGINE_HIVE_YARN_MEMORY_OVERHEAD, ENGINE_HIVE_YARN_QUEUE}
import org.apache.kyuubi.engine.hive.yarn.HiveYarnClient.{KYUUBI_HIVE_STAGING, KYUUBI_HIVE_YARN_APP_NAME, KYUUBI_HIVE_YARN_APP_TYPE}
import org.apache.kyuubi.util.KyuubiHadoopUtils



class HiveYarnClient(val args: ClientArguments, kyuubiConf: KyuubiConf) extends Logging {

  private val yarnClient = YarnClient.createYarnClient()
  private val hadoopConf = new YarnConfiguration(KyuubiHadoopUtils.newHadoopConf(kyuubiConf))
  private val memory = kyuubiConf.get(ENGINE_HIVE_YARN_MEMORY)
  private val memoryOverhead = kyuubiConf.get(ENGINE_HIVE_YARN_MEMORY_OVERHEAD)
  private val cores = kyuubiConf.get(ENGINE_HIVE_YARN_CORES)

  private var stagingDirPath: Path = _

  def submitApplication(): Unit = {
    var appId: ApplicationId = null
    try {
      yarnClient.init(hadoopConf)
      yarnClient.start()
      info("Requesting a new application from cluster with %d NodeManagers"
        .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

      // Get a new application from RM
      val newApp = yarnClient.createApplication()
      val newAppResponse = newApp.getNewApplicationResponse
      appId = newAppResponse.getApplicationId

      // TODO
      val appStagingBaseDir = kyuubiConf.getOption("TODO").map {
        new Path(_, UserGroupInformation.getCurrentUser.getShortUserName)
      }.getOrElse {
        FileSystem.get(hadoopConf).getHomeDirectory
      }
      stagingDirPath = new Path(appStagingBaseDir, getAppStagingDir(appId))

      // TODO
      new CallerContext("CLIENT", kyuubiConf.get(null), Option(appId.toString))

      verifyClusterResources(newAppResponse)

      val containerContext = createContainerLaunchContext()
    } catch {
      case e: Throwable =>
    }
  }

  private def createContainerLaunchContext(): ContainerLaunchContext = {
    info("Setting up container launch context for AM.")
    val launchEnv = setupLunchEnv()
    val localResources = prepareLocalResources()

    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    import scala.collection.JavaConverters._
    amContainer.setEnvironment(launchEnv.asJava)
    amContainer.setLocalResources(localResources.asJava)

    val javaOpts = ListBuffer[String]()
    javaOpts += "-Xmx" + memory + "m"
    javaOpts += "-Djava.io.tmpdir=" +
      buildPath(Environment.PWD.$$(), YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)
    null
  }

  /**
   * Setting up the context for submitting application master
   */
  private def createApplicationSubmissionContext(app: YarnClientApplication,
                                                 containerContext: ContainerLaunchContext):
  ApplicationSubmissionContext = {
    val appContext = app.getApplicationSubmissionContext
    appContext.setApplicationName(KYUUBI_HIVE_YARN_APP_NAME)
    appContext.setApplicationType(KYUUBI_HIVE_YARN_APP_TYPE)
    appContext.setQueue(kyuubiConf.get(ENGINE_HIVE_YARN_QUEUE))
    appContext.setAMContainerSpec(containerContext)
    appContext.setMaxAppAttempts(kyuubiConf.get(ENGINE_HIVE_YARN_MAX_ATTEMPTS))
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemorySize(memory + memoryOverhead)
    capability.setVirtualCores(cores)
    appContext.setResource(capability)
    appContext
  }



  private def setupLunchEnv(): mutable.HashMap[String, String] = {
    info("Setting up the launch environment for AM container.")
    val env = new mutable.HashMap[String, String]()

    env
  }

  private def prepareLocalResources(): mutable.HashMap[String, LocalResource] = {
    null
  }



  private def verifyClusterResources(newAppResponse: GetNewApplicationResponse): Unit = {

  }

  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(KYUUBI_HIVE_STAGING, appId.toString)
  }

  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }
}

private object HiveYarnClient extends Logging {

  val KYUUBI_HIVE_STAGING: String = ".kyuubiHiveStaging"
  val KYUUBI_HIVE_YARN_APP_NAME: String = "KYUUBI-HIVE-SERVER"
  val KYUUBI_HIVE_YARN_APP_TYPE: String = "KYUUBI-HIVE"
}
