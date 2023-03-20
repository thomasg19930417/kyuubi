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

package org.apache.kyuubi.plugin.lineage

import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

import org.apache.kyuubi.plugin.lineage.helper.SparkSQLLineageParseHelper

class SparkOperationLineageQueryExecutionListener extends QueryExecutionListener {

  private lazy val dispatchers: Seq[LineageDispatcher] = {
    SparkContextHelper.getConf(LineageConf.DISPATCHERS).map(LineageDispatcher(_))
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    val maybeTuple: Option[(LineageDDL, Lineage)] = {
      SparkSQLLineageParseHelper(qe.sparkSession).transformToLineage(qe.id, qe.optimizedPlan)
    }
    dispatchers.foreach(_.send(qe, Some(maybeTuple.get._1), Some(maybeTuple.get._2)))
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    dispatchers.foreach(_.onFailure(qe, exception))
  }
}
