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

package org.apache.kyuubi.plugin.lineage.event

import java.util.TimeZone

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.kyuubi.Logging
import org.apache.kyuubi.events.KyuubiEvent
import org.apache.kyuubi.events.handler.EventHandler
import org.apache.kyuubi.lineage.db.entity.{TableColumnInfo, TableColumnLineageInfo, TableInfo, TableLineageInfo}
import org.apache.kyuubi.lineage.db.entity.TableInfo.TableInfoBuilder
import org.apache.kyuubi.lineage.db.mapper.{TableColumnLineageInfoMapper, TableColumnMapper, TableInfoMapper, TableLineageInfoMapper}
import org.apache.kyuubi.plugin.lineage._
import org.apache.kyuubi.plugin.lineage.dispatcher.OperationLineageKyuubiEvent
import org.apache.kyuubi.plugin.lineage.ibatis.IbatisHelper

/**
 * *
 * Write SparkSql Lineage to mysql db
 */

class MysqlEventHandle extends EventHandler[KyuubiEvent] with Logging {

  val mapper = new IbatisHelper() {}

  val userName = UserGroupInformation.getCurrentUser.getShortUserName

  val DELETE_FLAG = "yyyyMMddHHmmss"

  val dsId = SparkContextHelper.getConf(LineageConf.LINEAGE_REALM).toInt

  // catch 所有异常
  override def apply(event: KyuubiEvent): Unit = {
    event match {
      case event: OperationLineageKyuubiEvent =>
        val lineage: Option[Lineage] = event.lineage
        val operator: Option[LineageDDL] = event.ddlOperator
        writeDDLOperatorToDb(operator)
        writeLineageToDB(lineage)
      case _ => // no op
    }
  }

  def writeLineageToDB(lineage: Option[Lineage]): Unit = {

    lineage.foreach(dataLineage => {
      if (null != dataLineage.inputTables && null != dataLineage.outputTables &&
        dataLineage.inputTables.size > 0 && dataLineage.outputTables.size > 0) {
        // 需要从库中查询表或者字段主键ID ，这里直接根据表查询表以及字段信息
        // 需要生成id -表名/字段名 mapping 映射 然后根据映射组装id映射 写入库中
        val tbs: List[String] = dataLineage.outputTables ++ dataLineage.inputTables
        var idMapping = mutable.Map[String, Long]()
        tbs.foreach(item => {
          val tableItem = item.split("\\.")
          val mapping: mutable.Map[String, Long] =
            fetchTableAndColumnMapping(tableItem(0), tableItem(1))
          idMapping = idMapping ++ mapping
        })
        // outTable or outColumn  => inputTable or inputColumn
        val tuple: (List[(Long, Long)], List[(Long, Long)]) =
          tranformLineage(dataLineage, idMapping)
        // save
        saveLineageToDb(tuple._1, tuple._2)
      }
    })
  }

  def saveLineageToDb(tbLineages: List[(Long, Long)], columnLineages: List[(Long, Long)]): Unit = {
    mapper.withSession(session => {
      try {
        val date = getDateFromTimeStamp(System.currentTimeMillis())
        session.getConnection.setAutoCommit(false)
        val tableLineageMapper = session.getMapper(classOf[TableLineageInfoMapper])
        val columnLinageMapper = session.getMapper(classOf[TableColumnLineageInfoMapper])

        val tableLinageEntitys = tbLineages.map(item => {
          new TableLineageInfo(item._2, item._1, date, date)
        })

        val columnLinageEntitys =
          columnLineages.map(item => new TableColumnLineageInfo(item._2, item._1, date, date))
        tableLineageMapper.saveOrUpdate(tableLinageEntitys.asJava)
        columnLinageMapper.saveOrUpdate(columnLinageEntitys.asJava)
        session.commit()
      } catch {
        case e: Exception =>
          session.rollback()
          warn(s"Write Lineage to db failed", e)
          throw new Throwable(e)
      } finally {
        session.close()
      }

    })
  }

  def fetchTableAndColumnMapping(dbName: String, tbName: String): mutable.Map[String, Long] = {
    val idMapping = mutable.Map[String, Long]()
    mapper.withSession(session => {
      try {
        val tableMapper = session.getMapper(classOf[TableInfoMapper])
        val queryWrapper = getQueryWrapper[TableInfo](Seq(
          ("ds_id", dsId),
          ("database_name", dbName),
          ("table_ename", tbName)))
        queryWrapper.select("id", "table_ename")
        val tableInfo = tableMapper.selectOne(queryWrapper)
        assert(null != tableInfo, s"Can't find table ${dbName}.${tbName} info")
        val columnMapper = session.getMapper(classOf[TableColumnMapper])
        val columnQueryWrapper =
          getQueryWrapper[TableColumnInfo](Seq(("table_id", tableInfo.getId)))
        columnQueryWrapper.select("id", "field_ename")
        val columns = columnMapper.selectList(columnQueryWrapper)
        idMapping.put(s"${dbName}.${tbName}", tableInfo.getId)
        columns.asScala.map(columnInfo =>
          idMapping.put(s"${dbName}.${tbName}.${columnInfo.getFieldEname}", columnInfo.getId))
        idMapping
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw new Exception(e.getMessage)
      } finally {
        session.close()
      }
    })
  }

  def tranformLineage(
      lineage: Lineage,
      idMapping: mutable.Map[String, Long]): (List[(Long, Long)], List[(Long, Long)]) = {
    val inTables = lineage.inputTables
    val outTables = lineage.outputTables
    // 只处理存在输入输出表的情况

    val columnLineage = lineage.columnLineage

    // 输入表 输出表可能都是多个   输出表 -> 输入表
    val tableLinages: List[(Long, Long)] = outTables.flatMap(outTb => {
      inTables.map(intb => {
        (idMapping.getOrElse(outTb, -1L), idMapping.getOrElse(intb, -1L))
      })
    }).filter(item => {
      item._1 != -1L && item._2 != -1L
    })

    // write column lineage
    val columnLineages: List[(Long, Long)] = columnLineage.flatMap(columnLinage => {
      columnLinage.originalColumns.map(sourceColumn => {
        (idMapping.getOrElse(columnLinage.column, -1L), idMapping.getOrElse(sourceColumn, -1L))
      })
    }).filter(item => {
      item._1 != -1L && item._2 != -1L
    })
    (tableLinages, columnLineages)
  }

  // 1.血缘清理时只在删除表时对表血缘做清理(不会影响后续的逻辑)
  // 2.暂时不写入审计表(目前看alter的概率极小)
  // 3.没有对replace做兼容，只有常规表名，字段的修改以及表的删除
  def writeDDLOperatorToDb(operator: Option[LineageDDL]): Unit = {
    operator.foreach {
      case null => // no op
      case LineageDDL(CreateTableOperator(tableInfo)) =>
        mapper.withSession(session => {
          try {
            session.getConnection.setAutoCommit(false)
            val tableMapper = session.getMapper(classOf[TableInfoMapper])
            tableMapper.insert(tableInfo)
            val columnMapper = session.getMapper(classOf[TableColumnMapper])
            tableInfo.getTableColumnInfoList.asScala.foreach(_.setTableId(tableInfo.getId))
            columnMapper.insertBatchSomeColumn(tableInfo.getTableColumnInfoList)
            session.commit()
          } catch {
            case e: Exception =>
              session.rollback()
              warn("Write Create TableOperator failed", e)
          } finally {
            session.close()
          }
        })

      case LineageDDL(CreateTableLikeOperator(source, target)) =>
        mapper.withSession(session => {
          try {
            val date = getDateFromTimeStamp(System.currentTimeMillis())

            def copyData(srcTable: TableInfo, destTable: TableIdentifier): Unit = {
              srcTable.setTableEname(destTable.table)
              srcTable.setDatabaseName(destTable.database.getOrElse("default"))
              srcTable.setTableOwner(userName)
              srcTable.setCreateTime(date)
              srcTable.setUpdateTime(date)
              srcTable.setId(null) // auto increment
            }

            session.getConnection.setAutoCommit(false)
            val tableMapper = session.getMapper(classOf[TableInfoMapper])
            val info = queryTableWithInfo(source.database.getOrElse("default"), source.table)
            val queryWrapper = getQueryWrapper[TableInfo](info)
            val sourceTableInfo = tableMapper.selectOne(queryWrapper)
            val sourceTableId = sourceTableInfo.getId

            assert(null != sourceTableInfo, "can not get table info from metadb")
            copyData(sourceTableInfo, target)
            tableMapper.insert(sourceTableInfo)

            val columnMapper = session.getMapper(classOf[TableColumnMapper])
            val columnWrapper = getQueryWrapper[TableColumnInfo](Seq(
              ("table_id", sourceTableId),
              ("deleted", 0)))
            val columns = columnMapper.selectList(columnWrapper)
            assert(
              null != columns && columns.size() > 0,
              s"can't find table ${sourceTableInfo.getTableEname} columns info")
            columns.asScala.foreach(column => {
              column.setId(null) // auto increment
              column.setTableId(sourceTableInfo.getId)
              column.setCreateTime(sourceTableInfo.getCreateTime)
              column.setUpdateTime(sourceTableInfo.getUpdateTime)
            })
            columnMapper.insertBatchSomeColumn(columns)
            session.commit()
          } catch {
            case e: Exception =>
              session.rollback()
              warn(s"Write CreateTableLikeOperator failed, msg: ${e.getMessage}", e)
          } finally {
            session.close()
          }
        })

      case LineageDDL(DropTableOperator(table)) =>
        mapper.withSession(session => {
          try {
            session.getConnection.setAutoCommit(false)
            val info = queryTableWithInfo(table.database.getOrElse("default"), table.table)
            val queryWrapper = getQueryWrapper[TableInfo](info)
            val tableInfoMapper = session.getMapper(classOf[TableInfoMapper])
            val tableInfo = tableInfoMapper.selectOne(queryWrapper)
            val deleteFlag =
              s"""_LINEAGE_DEL_${getDateFromTimeStamp(System.currentTimeMillis(), DELETE_FLAG)}"""
            tableInfo.setTableEname(s"${table.table}${deleteFlag}")
            tableInfo.setDeleted(1)
            tableInfoMapper.updateById(tableInfo)

            val tableColumnMapper = session.getMapper(classOf[TableColumnMapper])
            tableColumnMapper.deleteFieldByTableId(tableInfo.getId, deleteFlag)

            // delete lineage info, 只需要删除表血缘就可以保证正常工作
            val tableLineageMapper = session.getMapper(classOf[TableLineageInfoMapper])
            tableLineageMapper.logicalDeletedLineage(tableInfo.getId)
            session.commit()
          } catch {
            case e: Exception =>
              session.rollback()
              warn(s"Write DropTableOperator failed[${table.toString()}], msg: ${e.getMessage}", e)
          } finally {
            session.close()
          }
        })

      case LineageDDL(AlterTableRenameOperator(oldTable, newTable)) =>
        mapper.withSession(session => {
          try {
            val tableInfoMapper = session.getMapper(classOf[TableInfoMapper])
            val tableInfo = new TableInfoBuilder().setTableEname(newTable.table)
              .setDatabaseName(newTable.database.getOrElse("default"))
              .setUpdateTime(getDateFromTimeStamp(System.currentTimeMillis()))
              .builder()
            val updateWrapper = getUpdateWrapper[TableInfo](Seq(
              ("ds_id", dsId),
              ("database_name", oldTable.database.getOrElse("default")),
              ("table_ename", oldTable.table)))
            tableInfoMapper.update(tableInfo, updateWrapper)
            session.commit()
          } catch {
            case e: Exception =>
              warn(
                s"Write AlterTableRenameOperator failed[${oldTable.toString()} => ${newTable.toString()}]," +
                  s" msg: ${e.getMessage}",
                e)
          } finally {
            session.close()
          }
        })

      case LineageDDL(AlterTableAddColumnsOperator(table, columns)) =>
        mapper.withSession(session => {
          try {
            session.getConnection.setAutoCommit(false)
            // 获取当前index信息
            val columnMapper = session.getMapper(classOf[TableColumnMapper])
            val index = columnMapper.getTableColumnsCount(
              dsId,
              table.database.getOrElse("default"),
              table.table)
            val date = getDateFromTimeStamp(System.currentTimeMillis())
            // 查询表信息
            val tableMapper = session.getMapper(classOf[TableInfoMapper])
            val tableId =
              tableMapper.getTableId(dsId, table.database.getOrElse("default"), table.table)

            // 补充字段信息
            columns.zipWithIndex.foreach(column => {
              val columnInfo = column._1
              columnInfo.setTableId(tableId)
              columnInfo.setIndex((index + column._2))
              columnInfo.setCreateTime(date)
              columnInfo.setUpdateTime(date)
            })

            // save column info
            columnMapper.insertBatchSomeColumn(columns.toList.asJava)
            session.commit()
          } catch {
            case e: Exception =>
              session.rollback()
              warn(
                s"Write AlterTableAddColumnsOperator faild[${table.toString()}], msg: ${e.getMessage}",
                e)
          } finally {
            session.close()
          }
        })
      //  only update name type comment and time
      case LineageDDL(AlterTableChangeColumnOperator(table, oldColumnName, column)) =>
        mapper.withSession(session => {
          val tableInfoMapper = session.getMapper(classOf[TableInfoMapper])
          val tableId =
            tableInfoMapper.getTableId(dsId, table.database.getOrElse("default"), table.table)
          val columnMapper = session.getMapper(classOf[TableColumnMapper])
          val updateWrapper = getUpdateWrapper[TableColumnInfo](Seq(
            ("table_id", tableId),
            ("field_ename", oldColumnName)))
          updateWrapper.set("field_cname", column.getFieldCname)
          columnMapper.update(null, updateWrapper)
          session.commit()
        })

      case _ => // no op
    }
  }

  def queryTableWithInfo(database: String, tableName: String): TableInfo = {
    new TableInfoBuilder()
      .setDsId(dsId)
      .setDatabaseName(database)
      .setTableEname(tableName)
      .builder()
  }

  def getDateFromTimeStamp(time: Long): String = {
    DateFormatUtils.format(time, "yyyy-MM-dd HH:mm:ss", TimeZone.getDefault)
  }

  def getQueryWrapper[T](eqCond: Seq[(String, Any)]): QueryWrapper[T] = {
    val queryWrapper = new QueryWrapper[T]()
    eqCond.foreach(item => queryWrapper.eq(item._1, item._2))
    queryWrapper
  }

  def getQueryWrapper[T](t: T): QueryWrapper[T] = {
    val queryWrapper = new QueryWrapper[T]()
    queryWrapper.setEntity(t)
    queryWrapper
  }

  def getUpdateWrapper[T](eqCond: Seq[(String, Any)]): UpdateWrapper[T] = {
    val updateWrapper = new UpdateWrapper[T]()
    eqCond.foreach(item => updateWrapper.eq(item._1, item._2))
    updateWrapper
  }

  def getDateFromTimeStamp(time: Long, pattern: String): String = {
    DateFormatUtils.format(time, pattern, TimeZone.getDefault)
  }

}
