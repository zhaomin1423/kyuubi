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

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.ipc.CallerContext
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records.{ApplicationId, ContainerLaunchContext}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.kyuubi.Logging
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.engine.hive.yarn.HiveYarnClient.KYUUBI_HIVE_STAGING
import org.apache.kyuubi.util.KyuubiHadoopUtils

import scala.collection.mutable

class HiveYarnClient(val args: ClientArguments, kyuubiConf: KyuubiConf) extends Logging {

  private val yarnClient = YarnClient.createYarnClient()
  private val hadoopConf = new YarnConfiguration(KyuubiHadoopUtils.newHadoopConf(kyuubiConf))

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

      val containerContext = createContainerLaunchContext(newAppResponse)
    } catch {
      case e: Throwable =>
    }
  }

  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse):
  ContainerLaunchContext = {
    info("Setting up container launch context for AM.")
    val appId = newAppResponse.getApplicationId

  }

  private def setupLunchEnv(stagingDirPath: Path): mutable.HashMap[String, String] = {
    info("Setting up the launch environment for AM container.")
    val env = new mutable.HashMap[String, String]()

    env
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
}
