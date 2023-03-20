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

package org.apache.kyuubi.plugin.lineage.helper

import java.util.TimeZone

import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, PersistedView, ViewType}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.hive.execution.{CreateHiveTableAsSelectCommand, OptimizedCreateHiveTableAsSelectCommand}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import org.apache.kyuubi.lineage.db.entity.{TableColumnInfo, TableInfo}
import org.apache.kyuubi.lineage.db.entity.TableInfo.TableInfoBuilder
import org.apache.kyuubi.plugin.lineage._
import org.apache.kyuubi.plugin.lineage.helper.SparkListenerHelper.isSparkVersionAtMost

trait LineageParser {
  def sparkSession: SparkSession

  val SUBQUERY_COLUMN_IDENTIFIER = "__subquery__"
  val AGGREGATE_COUNT_COLUMN_IDENTIFIER = "__count__"
  val LOCAL_TABLE_IDENTIFIER = "__local__"

  type AttributeMap[A] = ListMap[Attribute, A]

  def parse(plan: LogicalPlan): Lineage = {
    val columnsLineage =
      extractColumnsLineage(plan, ListMap[Attribute, AttributeSet]()).toList.collect {
        case (k, attrs) =>
          k.name -> attrs.map(_.qualifiedName).toSet
      }
    val (inputTables, outputTables) = columnsLineage.foldLeft((List[String](), List[String]())) {
      case ((inputs, outputs), (out, in)) =>
        val x = (inputs ++ in.map(_.split('.').init.mkString("."))).filter(_.nonEmpty)
        val y = outputs ++ List(out.split('.').init.mkString(".")).filter(_.nonEmpty)
        (x, y)
    }
    Lineage(inputTables.distinct, outputTables.distinct, columnsLineage)
  }

  private def mergeColumnsLineage(
      left: AttributeMap[AttributeSet],
      right: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {
    left ++ right.map {
      case (k, attrs) =>
        k -> (attrs ++ left.getOrElse(k, AttributeSet.empty))
    }
  }

  private def joinColumnsLineage(
      parent: AttributeMap[AttributeSet],
      child: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {
    if (parent.isEmpty) child
    else {
      val childMap = child.map { case (k, attrs) => (k.exprId, attrs) }
      parent.map { case (k, attrs) =>
        k -> AttributeSet(attrs.flatMap(attr =>
          childMap.getOrElse(
            attr.exprId,
            if (attr.name.equalsIgnoreCase(AGGREGATE_COUNT_COLUMN_IDENTIFIER)) AttributeSet(attr)
            else AttributeSet.empty)))
      }
    }
  }

  private def getExpressionSubqueryPlans(expression: Expression): Seq[LogicalPlan] = {
    expression match {
      case s: ScalarSubquery => Seq(s.plan)
      case s => s.children.flatMap(getExpressionSubqueryPlans)
    }
  }

  private def findSparkPlanLogicalLink(sparkPlans: Seq[SparkPlan]): Option[LogicalPlan] = {
    sparkPlans.find(_.logicalLink.nonEmpty) match {
      case Some(sparkPlan) => sparkPlan.logicalLink
      case None => findSparkPlanLogicalLink(sparkPlans.flatMap(_.children))
    }
  }

  private def containsCountAll(expr: Expression): Boolean = {
    expr match {
      case e: Count if e.references.isEmpty => true
      case e =>
        e.children.exists(containsCountAll)
    }
  }

  private def getSelectColumnLineage(
      named: Seq[NamedExpression]): AttributeMap[AttributeSet] = {
    val exps = named.map {
      case exp: Alias =>
        val references =
          if (exp.references.nonEmpty) exp.references
          else {
            val attrRefs = getExpressionSubqueryPlans(exp.child)
              .map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]()))
              .foldLeft(ListMap[Attribute, AttributeSet]())(mergeColumnsLineage).values
              .foldLeft(AttributeSet.empty)(_ ++ _)
              .map(attr => attr.withQualifier(attr.qualifier :+ SUBQUERY_COLUMN_IDENTIFIER))
            AttributeSet(attrRefs)
          }
        (
          exp.toAttribute,
          if (!containsCountAll(exp.child)) references
          else references + exp.toAttribute.withName(AGGREGATE_COUNT_COLUMN_IDENTIFIER))
      case a: Attribute => a -> a.references
    }
    ListMap(exps: _*)
  }

  private def joinRelationColumnLineage(
      parent: AttributeMap[AttributeSet],
      relationAttrs: Seq[Attribute],
      qualifier: Seq[String]): AttributeMap[AttributeSet] = {
    val relationAttrSet = AttributeSet(relationAttrs)
    if (parent.nonEmpty) {
      parent.map { case (k, attrs) =>
        k -> AttributeSet(attrs.collect {
          case attr if relationAttrSet.contains(attr) =>
            attr.withQualifier(qualifier)
          case attr
              if attr.qualifier.nonEmpty && attr.qualifier.last.equalsIgnoreCase(
                SUBQUERY_COLUMN_IDENTIFIER) =>
            attr.withQualifier(attr.qualifier.init)
          case attr if attr.name.equalsIgnoreCase(AGGREGATE_COUNT_COLUMN_IDENTIFIER) =>
            attr.withQualifier(qualifier)
        })
      }
    } else {
      ListMap(relationAttrs.map { attr =>
        (
          attr,
          AttributeSet(attr.withQualifier(qualifier)))
      }: _*)
    }
  }

  private def mergeRelationColumnLineage(
      parentColumnsLineage: AttributeMap[AttributeSet],
      relationOutput: Seq[Attribute],
      relationColumnLineage: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {
    val mergedRelationColumnLineage = {
      relationOutput.foldLeft((ListMap[Attribute, AttributeSet](), relationColumnLineage)) {
        case ((acc, x), attr) =>
          (acc + (attr -> x.head._2), x.tail)
      }._1
    }
    joinColumnsLineage(parentColumnsLineage, mergedRelationColumnLineage)
  }

  private def extractColumnsLineage(
      plan: LogicalPlan,
      parentColumnsLineage: AttributeMap[AttributeSet]): AttributeMap[AttributeSet] = {

    plan match {
      // For command
      case p if p.nodeName == "CommandResult" =>
        val commandPlan = getPlanField[LogicalPlan]("commandLogicalPlan", plan)
        extractColumnsLineage(commandPlan, parentColumnsLineage)
      case p if p.nodeName == "AlterViewAsCommand" =>
        val query =
          if (isSparkVersionAtMost("3.1")) {
            sparkSession.sessionState.analyzer.execute(getQuery(plan))
          } else {
            getQuery(plan)
          }
        val view = getPlanField[TableIdentifier]("name", plan).unquotedString
        extractColumnsLineage(query, parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$view.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateViewCommand"
            && getPlanField[ViewType]("viewType", plan) == PersistedView =>
        val view = getPlanField[TableIdentifier]("name", plan).unquotedString
        val outputCols =
          getPlanField[Seq[(String, Option[String])]]("userSpecifiedColumns", plan).map(_._1)
        val query =
          if (isSparkVersionAtMost("3.1")) {
            sparkSession.sessionState.analyzer.execute(getPlanField[LogicalPlan]("child", plan))
          } else {
            getPlanField[LogicalPlan]("plan", plan)
          }

        extractColumnsLineage(query, parentColumnsLineage).zipWithIndex.map {
          case ((k, v), i) if outputCols.nonEmpty => k.withName(s"$view.${outputCols(i)}") -> v
          case ((k, v), _) => k.withName(s"$view.${k.name}") -> v
        }

      case p if p.nodeName == "CreateDataSourceTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("table", plan).qualifiedName
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateHiveTableAsSelectCommand" ||
            p.nodeName == "OptimizedCreateHiveTableAsSelectCommand" =>
        val table = getPlanField[CatalogTable]("tableDesc", plan).qualifiedName
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "CreateTableAsSelect" ||
            p.nodeName == "ReplaceTableAsSelect" =>
        val (table, namespace, catalog) =
          if (isSparkVersionAtMost("3.2")) {
            (
              getPlanField[Identifier]("tableName", plan).name,
              getPlanField[Identifier]("tableName", plan).namespace.mkString("."),
              getPlanField[TableCatalog]("catalog", plan).name())
          } else {
            (
              getPlanMethod[Identifier]("tableName", plan).name(),
              getPlanMethod[Identifier]("tableName", plan).namespace().mkString("."),
              getCurrentPlanField[CatalogPlugin](
                getPlanMethod[LogicalPlan]("left", plan),
                "catalog").name())
          }
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(Seq(catalog, namespace, table, k.name).filter(_.nonEmpty).mkString(".")) -> v
        }

      case p if p.nodeName == "InsertIntoDataSourceCommand" =>
        val logicalRelation = getPlanField[LogicalRelation]("logicalRelation", plan)
        val table = logicalRelation.catalogTable.map(_.qualifiedName).getOrElse("")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if table.nonEmpty =>
            k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "InsertIntoHadoopFsRelationCommand" =>
        val table =
          getPlanField[Option[CatalogTable]]("catalogTable", plan).map(_.qualifiedName).getOrElse(
            "")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if table.nonEmpty =>
            k.withName(s"$table.${k.name}") -> v
        }

      case p
          if p.nodeName == "InsertIntoDataSourceDirCommand" ||
            p.nodeName == "InsertIntoHiveDirCommand" =>
        val dir =
          getPlanField[CatalogStorageFormat]("storage", plan).locationUri.map(_.toString).getOrElse(
            "")
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map {
          case (k, v) if dir.nonEmpty =>
            k.withName(s"`$dir`.${k.name}") -> v
        }

      case p if p.nodeName == "InsertIntoHiveTable" =>
        val table = getPlanField[CatalogTable]("table", plan).qualifiedName
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "SaveIntoDataSourceCommand" =>
        extractColumnsLineage(getQuery(plan), parentColumnsLineage)

      case p
          if p.nodeName == "AppendData"
            || p.nodeName == "OverwriteByExpression"
            || p.nodeName == "OverwritePartitionsDynamic" =>
        val table = getPlanField[NamedRelation]("table", plan).name
        extractColumnsLineage(getQuery(plan), parentColumnsLineage).map { case (k, v) =>
          k.withName(s"$table.${k.name}") -> v
        }

      case p if p.nodeName == "MergeIntoTable" =>
        val matchedActions = getPlanField[Seq[MergeAction]]("matchedActions", plan)
        val notMatchedActions = getPlanField[Seq[MergeAction]]("notMatchedActions", plan)
        val allAssignments = (matchedActions ++ notMatchedActions).collect {
          case UpdateAction(_, assignments) => assignments
          case InsertAction(_, assignments) => assignments
        }.flatten
        val nextColumnsLlineage = ListMap(allAssignments.map { assignment =>
          (
            assignment.key.asInstanceOf[Attribute],
            AttributeSet(assignment.value.asInstanceOf[Attribute]))
        }: _*)
        val targetTable = getPlanField[LogicalPlan]("targetTable", plan)
        val sourceTable = getPlanField[LogicalPlan]("sourceTable", plan)
        val targetColumnsLineage = extractColumnsLineage(
          targetTable,
          nextColumnsLlineage.map { case (k, _) => (k, AttributeSet(k)) })
        val sourceColumnsLineage = extractColumnsLineage(sourceTable, nextColumnsLlineage)
        val targetColumnsWithTargetTable = targetColumnsLineage.values.flatten.map { column =>
          column.withName(s"${column.qualifiedName}")
        }
        ListMap(targetColumnsWithTargetTable.zip(sourceColumnsLineage.values).toSeq: _*)

      // For query
      case p: Project =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.projectList))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Aggregate =>
        val nextColumnsLineage =
          joinColumnsLineage(parentColumnsLineage, getSelectColumnLineage(p.aggregateExpressions))
        p.children.map(extractColumnsLineage(_, nextColumnsLineage)).reduce(mergeColumnsLineage)

      case p: Join =>
        p.joinType match {
          case LeftSemi | LeftAnti =>
            extractColumnsLineage(p.left, parentColumnsLineage)
          case _ =>
            p.children.map(extractColumnsLineage(_, parentColumnsLineage))
              .reduce(mergeColumnsLineage)
        }

      case p: Union =>
        // merge all children in to one derivedColumns
        val childrenUnion =
          p.children.map(extractColumnsLineage(_, ListMap[Attribute, AttributeSet]())).map(
            _.values).reduce {
            (left, right) =>
              left.zip(right).map(attr => attr._1 ++ attr._2)
          }
        val childrenColumnsLineage = ListMap(p.output.zip(childrenUnion): _*)
        joinColumnsLineage(parentColumnsLineage, childrenColumnsLineage)

      case p: LogicalRelation if p.catalogTable.nonEmpty =>
        val tableName = p.catalogTable.get.qualifiedName
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      case p: HiveTableRelation =>
        val tableName = p.tableMeta.qualifiedName
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      case p: DataSourceV2ScanRelation =>
        val tableName = p.name
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      // For creating the view from v2 table, the logical plan of table will
      // be the `DataSourceV2Relation` not the `DataSourceV2ScanRelation`.
      // because the view from the table is not going to read it.
      case p: DataSourceV2Relation =>
        val tableName = p.name
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(tableName))

      case p: LocalRelation =>
        joinRelationColumnLineage(parentColumnsLineage, p.output, Seq(LOCAL_TABLE_IDENTIFIER))

      case p: InMemoryRelation =>
        // get logical plan from cachedPlan
        val cachedTableLogical = findSparkPlanLogicalLink(Seq(p.cacheBuilder.cachedPlan))
        cachedTableLogical match {
          case Some(logicPlan) =>
            val relationColumnLineage =
              extractColumnsLineage(logicPlan, ListMap[Attribute, AttributeSet]())
            mergeRelationColumnLineage(parentColumnsLineage, p.output, relationColumnLineage)
          case _ =>
            joinRelationColumnLineage(
              parentColumnsLineage,
              p.output,
              p.cacheBuilder.tableName.toSeq)
        }

      case p if p.children.isEmpty => ListMap[Attribute, AttributeSet]()

      case p =>
        p.children.map(extractColumnsLineage(_, parentColumnsLineage)).reduce(mergeColumnsLineage)
    }
  }

  private def getPlanField[T](field: String, plan: LogicalPlan): T = {
    getFieldVal[T](plan, field)
  }

  private def getCurrentPlanField[T](curPlan: LogicalPlan, field: String): T = {
    getFieldVal[T](curPlan, field)
  }

  private def getPlanMethod[T](name: String, plan: LogicalPlan): T = {
    getMethod[T](plan, name)
  }

  private def getQuery(plan: LogicalPlan): LogicalPlan = {
    getPlanField[LogicalPlan]("query", plan)
  }

  private def getFieldVal[T](o: Any, name: String): T = {
    Try {
      val field = o.getClass.getDeclaredField(name)
      field.setAccessible(true)
      field.get(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredFields.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
    }
  }

  private def getMethod[T](o: Any, name: String): T = {
    Try {
      val method = o.getClass.getDeclaredMethod(name)
      method.invoke(o)
    } match {
      case Success(value) => value.asInstanceOf[T]
      case Failure(e) =>
        val candidates = o.getClass.getDeclaredMethods.map(_.getName).mkString("[", ",", "]")
        throw new RuntimeException(s"$name not in $candidates", e)
    }
  }

}

case class SparkSQLLineageParseHelper(sparkSession: SparkSession) extends LineageParser
  with Logging {

  def transformToLineage(
      executionId: Long,
      plan: LogicalPlan): Option[(LineageDDL, Lineage)] = {
    Try((parserDDl(plan), parse(plan))).recover {
      case e: Exception =>
        logWarning(s"Extract Statement[$executionId] columns lineage failed.", e)
        throw e
    }.toOption
  }

  def getOutSchema(outAtt: Seq[Attribute], tableDesc: CatalogTable): CatalogTable = {
    val fields: Seq[StructField] =
      outAtt.map(att => StructField(att.name, att.dataType, true, att.metadata))
    val structType = StructType(fields)
    var tableDescNew = tableDesc
    if (tableDesc.dataSchema.fields.length <= 0) {
      tableDescNew = tableDesc.copy(schema = structType)
    }
    tableDescNew
  }

  def parserDDl(logicPaln: LogicalPlan): LineageDDL = {
    // 暂时只对hive相关的做处理
    logicPaln match {
      // create table
      case plan: CreateTableCommand =>
        LineageDDL(CreateTableOperator(transformToHiveTableInfo(plan.table)))
      case plan: CreateHiveTableAsSelectCommand =>
        val tableDesc = getOutSchema(plan.outputColumns, plan.tableDesc)
        LineageDDL(CreateTableOperator(transformToHiveTableInfo(tableDesc)))
      case plan: OptimizedCreateHiveTableAsSelectCommand =>
        val tableDesc = getOutSchema(plan.outputColumns, plan.tableDesc)
        LineageDDL(CreateTableOperator(transformToHiveTableInfo(tableDesc)))
      case plan: CreateTableLikeCommand =>
        LineageDDL(CreateTableLikeOperator(plan.sourceTable, plan.targetTable))

      // drop table
      case plan: DropTableCommand =>
        LineageDDL(DropTableOperator(plan.tableName))

      // alter table
      case plan: AlterTableRenameCommand =>
        LineageDDL(AlterTableRenameOperator(plan.oldName, plan.newName))

      case plan: AlterTableAddColumnsCommand =>
        val columns = plan.colsToAdd.map(transformToHiveTableColumnInfo(
          false,
          -1,
          _,
          getDateFromTimeStamp(System.currentTimeMillis())))
        LineageDDL(AlterTableAddColumnsOperator(plan.table, columns.toArray))

      case plan: AlterTableChangeColumnCommand =>
        LineageDDL(AlterTableChangeColumnOperator(
          plan.tableName,
          plan.columnName,
          transformToHiveTableColumnInfo(
            false,
            -1,
            plan.newColumn,
            getDateFromTimeStamp(System.currentTimeMillis()))))

      case plan =>
        if (plan.children.size <= 0) {
          null
        } else {
          plan.children.map(parserDDl(_)).head
        }

    }
  }

  val userName = UserGroupInformation.getCurrentUser.getShortUserName
  val dsId = SparkContextHelper.getConf(LineageConf.LINEAGE_REALM).toInt

  def transformToHiveTableInfo(table: CatalogTable): TableInfo = {
    val date = getDateFromTimeStamp(System.currentTimeMillis())
    val tableInfo = new TableInfoBuilder().setDsId(dsId)
      .setDatabaseName(table.identifier.database.getOrElse("default"))
      .setTableEname(table.identifier.table)
      .setTableCname(table.comment.getOrElse(""))
      .setCreateTime(date)
      .setUpdateTime(date)
      .setDeleted(0)
      .setPartition(table.partitionColumnNames.size > 0)
      .setStorageCapacity("-1")
      .setDataNum("-1")
      .setDescription(table.comment.getOrElse(""))

    Option(table.owner).filter(_.nonEmpty).orElse(Some(userName)).foreach(item => {
      tableInfo.setTableOwner(item)
    })

    // table field
    val dataColumnSchema: StructType = table.dataSchema
    val partColumnSchema: StructType = table.partitionSchema

    val dataColumns: Array[TableColumnInfo] = dataColumnSchema.fields.zipWithIndex.map(item =>
      transformToHiveTableColumnInfo(false, item._2, item._1, date))

    val partColumns: Array[TableColumnInfo] = partColumnSchema.fields.zipWithIndex.map(item =>
      transformToHiveTableColumnInfo(true, item._2 + dataColumns.size, item._1, date))
    import scala.collection.JavaConverters._
    tableInfo.setTableColumnInfoList((dataColumns ++ partColumns).toList.asJava)
    tableInfo.builder()
  }

  def transformToHiveTableColumnInfo(
      isPartition: Boolean,
      index: Int,
      field: StructField,
      date: String): TableColumnInfo = {

    val dataType: DataType = field.dataType
    val name: String = field.name
    val maybeString: Option[String] = field.getComment()
    val tableColumnInfo = new TableColumnInfo()
    tableColumnInfo.setFieldEname(name)
    tableColumnInfo.setFieldCname(maybeString.getOrElse(""))
    tableColumnInfo.setFieldType(dataType.simpleString)
    tableColumnInfo.setDeleted(0)
    tableColumnInfo.setCreateTime(date)
    tableColumnInfo.setUpdateTime(date)
    tableColumnInfo.setIndex(index)
    tableColumnInfo.setPartition(if (isPartition) 1 else 0)
    tableColumnInfo
  }

  def getDateFromTimeStamp(time: Long): String = {
    DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss", TimeZone.getDefault)
  }

}
