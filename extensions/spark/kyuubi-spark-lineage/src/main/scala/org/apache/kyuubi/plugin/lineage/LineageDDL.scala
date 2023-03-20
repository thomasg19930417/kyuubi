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

import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.kyuubi.lineage.db.entity.{TableColumnInfo, TableInfo}

class LineageDDL(val operator: Operator) {}

object LineageDDL {

  def apply(operator: Operator): LineageDDL = new LineageDDL(operator)

  def unapply(arg: LineageDDL): Option[Operator] = {
    Some(arg.operator)
  }
}

class Operator()

case class CreateTableOperator(tableInfo: TableInfo)
  extends Operator

case class CreateTableLikeOperator(sourceTable: TableIdentifier, targetTable: TableIdentifier)
  extends Operator

case class DropTableOperator(table: TableIdentifier)
  extends Operator

case class AlterTableRenameOperator(oldTable: TableIdentifier, newTable: TableIdentifier)
  extends Operator

case class AlterTableAddColumnsOperator(table: TableIdentifier, columns: Array[TableColumnInfo])
  extends Operator

case class AlterTableChangeColumnOperator(
    table: TableIdentifier,
    oldColumnName: String,
    column: TableColumnInfo) extends Operator
