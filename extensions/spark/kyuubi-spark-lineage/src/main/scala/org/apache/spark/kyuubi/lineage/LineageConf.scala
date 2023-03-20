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

package org.apache.spark.kyuubi.lineage

import org.apache.spark.internal.config.ConfigBuilder

import org.apache.kyuubi.plugin.lineage.LineageDispatcherType

object LineageConf {

  val DISPATCHERS = ConfigBuilder("spark.kyuubi.plugin.lineage.dispatchers").stringConf
    .toSequence
    .checkValue(
      _.toSet.subsetOf(LineageDispatcherType.values.map(_.toString)),
      "Unsupported lineage dispatchers")
    .createWithDefault(Seq(LineageDispatcherType.SPARK_EVENT.toString))

  val LINEAGE_URL = ConfigBuilder("spark.kyuubi.plugin.lineage.db.url").stringConf
    .createWithDefaultString("")

  val LINEAGE_PASSWD = ConfigBuilder("spark.kyuubi.plugin.lineage.db.password").stringConf
    .createWithDefaultString("")

  val LINEAGE_SERVER = ConfigBuilder("spark.kyuubi.plugin.lineage.db.host").stringConf
    .createWithDefaultString("")

  val LINEAGE_DB = ConfigBuilder("spark.kyuubi.plugin.lineage.db.db").stringConf
    .createWithDefaultString("")

  val LINEAGE_REALM = ConfigBuilder("spark.kyuubi.plugin.lineage.db.realm").stringConf
    .createWithDefaultString("")

  val LINEAGE_USER = ConfigBuilder("spark.kyuubi.plugin.lineage.db.user").stringConf
    .createWithDefaultString("")

}
