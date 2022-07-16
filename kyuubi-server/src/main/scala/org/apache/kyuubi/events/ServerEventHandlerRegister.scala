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
package org.apache.kyuubi.events

import java.net.InetAddress

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf.SERVER_EVENT_JSON_LOG_PATH
import org.apache.kyuubi.events.handler.{EventHandler, ServerJsonLoggingEventHandler}
import org.apache.kyuubi.util.KyuubiHadoopUtils

object ServerEventHandlerRegister extends EventHandlerRegister {

  override protected def createJsonEventHandler(kyuubiConf: KyuubiConf)
      : Option[EventHandler[KyuubiEvent]] = {
    val hadoopConf = KyuubiHadoopUtils.newHadoopConf(kyuubiConf)
    val hostName = InetAddress.getLocalHost.getCanonicalHostName
    val handler = ServerJsonLoggingEventHandler(
      s"server-$hostName",
      SERVER_EVENT_JSON_LOG_PATH,
      hadoopConf,
      kyuubiConf)
    Option.apply(handler)
  }
}
