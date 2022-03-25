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

import scala.collection.mutable.ArrayBuffer

class ClientArguments(args: Array[String]) {

  var jar: String = null
  var mainClass: String = null
  var userArgs: ArrayBuffer[String] = new ArrayBuffer[String]()

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (!args.isEmpty) {
      args match {
        case "--jars" :: value :: tail =>
          jar = value
          args = tail
        case _ =>
          throw new IllegalArgumentException(getUsageMessage(args))
      }
    }
  }

  private def getUsageMessage(unknownParam: List[String] = null): String = {
    val message = if (unknownParam != null) s"Unknown/unsupported param $unknownParam\n" else ""
    message
  }

}
