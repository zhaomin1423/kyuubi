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
package org.apache.kyuubi.it.jdbc

import org.apache.kyuubi.{Utils, WithKyuubiServer}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._
import org.apache.kyuubi.engine.jdbc.doris.WithDorisEngine


trait WithKyuubiServerAndDorisContainer extends WithKyuubiServer with WithDorisEngine {

  private val kyuubiHome: String = Utils
    .getCodeSourceLocation(getClass).split("integration-tests").head

  override protected val conf: KyuubiConf = {
    KyuubiConf()
      .set(s"$KYUUBI_ENGINE_ENV_PREFIX.$KYUUBI_HOME", kyuubiHome)
  }

  override def beforeAll(): Unit = {
    val configs = withKyuubiConf
    configs.foreach(config => conf.set(config._1, config._2))
    super.beforeAll()
  }
}
