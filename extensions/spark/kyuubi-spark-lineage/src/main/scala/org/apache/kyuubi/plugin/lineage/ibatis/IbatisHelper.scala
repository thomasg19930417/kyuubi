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
package org.apache.kyuubi.plugin.lineage.ibatis

import java.io.{File, FileInputStream, InputStream}
import java.net.URL
import java.util
import java.util.jar.{JarEntry, JarFile}

import com.baomidou.mybatisplus.core.MybatisConfiguration
import com.baomidou.mybatisplus.core.injector.{AbstractMethod, DefaultSqlInjector}
import com.baomidou.mybatisplus.core.mapper.BaseMapper
import com.baomidou.mybatisplus.core.metadata.TableInfo
import com.baomidou.mybatisplus.core.toolkit.GlobalConfigUtils
import com.baomidou.mybatisplus.extension.injector.methods.InsertBatchSomeColumn
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor
import com.baomidou.mybatisplus.extension.plugins.handler.TableNameHandler
import com.baomidou.mybatisplus.extension.plugins.inner.DynamicTableNameInnerInterceptor
import com.mysql.cj.jdbc.MysqlDataSource
import org.apache.ibatis.builder.xml.XMLMapperBuilder
import org.apache.ibatis.logging.stdout.StdOutImpl
import org.apache.ibatis.mapping.Environment
import org.apache.ibatis.plugin.Interceptor
import org.apache.ibatis.session.{SqlSession, SqlSessionFactoryBuilder}
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory
import org.apache.spark.kyuubi.lineage.{LineageConf, SparkContextHelper}
import sun.net.www.protocol.jar.JarURLConnection

trait IbatisHelper {

  lazy val datasource = {
    val dataSource = new MysqlDataSource()
    dataSource.setUser(SparkContextHelper.getConf(LineageConf.LINEAGE_USER))
    dataSource.setPassword(SparkContextHelper.getConf(LineageConf.LINEAGE_PASSWD))
    dataSource.setServerName(SparkContextHelper.getConf(LineageConf.LINEAGE_SERVER))
    dataSource.setDatabaseName(SparkContextHelper.getConf(LineageConf.LINEAGE_DB))
    dataSource.setURL(s"${dataSource.getURL}?useUnicode=true&autoReconnect=true")
    dataSource
  }

  val realmMap = Map(
    "dgm_field_info_38" -> "dgm_field_info_yz",
    "dgm_field_info_36" -> "dgm_field_info_zw",
    "table_column_lineage_38" -> "dgm_field_lineage_yz",
    "table_column_lineage_36" -> "dgm_field_lineage_zw")

  lazy val sessionFactory = {

    val config = GlobalConfigUtils.defaults()
    config.setSqlInjector(new EasySqlInjector())
    config.setSuperMapperClass(classOf[BaseMapper[_]])
    val sqlSessionFactoryBuilder = new SqlSessionFactoryBuilder()
    val configuration = new MybatisConfiguration()

    // 在addMapper时会同时解析 SqlInjector 添加相关的方法，因此这里需要放在addMapper之前.
    // 否则不会生效
    GlobalConfigUtils.setGlobalConfig(configuration, config)
    initConfiguration(configuration)

    val environment = new Environment("lineage_spark", new JdbcTransactionFactory(), datasource)
    configuration.setEnvironment(environment)
    this.registryMapperXml(configuration)
    sqlSessionFactoryBuilder.build(configuration)
  }

  def registryMapperXml(configuration: MybatisConfiguration): Unit = {
    val contextLoader = Thread.currentThread().getContextClassLoader
    val mapper = contextLoader.getResources("mapper")
    while (mapper.hasMoreElements) {
      val url: URL = mapper.nextElement()
      if (url.getProtocol.equalsIgnoreCase("file")) {
        val path: String = url.getPath
        val file = new File(path)
        val files: Array[File] = file.listFiles()
        files.foreach(curFile => {
          val stream = new FileInputStream(curFile)
          new XMLMapperBuilder(
            stream,
            configuration,
            curFile.getPath,
            configuration.getSqlFragments).parse()
          stream.close()
        })
      } else {
        val jarUrlConnection = url.openConnection().asInstanceOf[JarURLConnection]
        val file: JarFile = jarUrlConnection.getJarFile
        val jarEntity = file.entries()
        while (jarEntity.hasMoreElements) {
          val entry: JarEntry = jarEntity.nextElement()
          if (entry.getName.endsWith("xml")) {
            val stream: InputStream = file.getInputStream(entry)
            new XMLMapperBuilder(
              stream,
              configuration,
              entry.getName,
              configuration.getSqlFragments).parse()
            stream.close()
          }
        }
      }
    }

  }

  def initConfiguration(configuration: MybatisConfiguration): Unit = {
    configuration.setMapUnderscoreToCamelCase(true)
    configuration.setUseGeneratedKeys(true)
    configuration.setLogImpl(classOf[StdOutImpl])
    configuration.addMappers("org.apache.kyuubi.lineage.db.mapper")
    configuration.addInterceptor(getInterceptor())
  }

  def getInterceptor(): Interceptor = {
    val interceptor = new MybatisPlusInterceptor()
    val dynamicTableName = new DynamicTableNameInnerInterceptor()
    dynamicTableName.setTableNameHandler(new TableNameHandler {
      override def dynamicTableName(sql: String, tableName: String): String = {
        realmMap.get(
          s"${tableName}_${SparkContextHelper.getConf(LineageConf.LINEAGE_REALM)}").getOrElse(
          tableName)
      }
    })
    interceptor.addInnerInterceptor(dynamicTableName)
    interceptor
  }

  def withSession[T](f: SqlSession => T): T = {
    val session: SqlSession = sessionFactory.openSession()
    f(session)
  }

  class EasySqlInjector extends DefaultSqlInjector {
    override def getMethodList(
        mapperClass: Class[_],
        tableInfo: TableInfo): util.List[AbstractMethod] = {
      val methods = super.getMethodList(mapperClass, tableInfo)
      methods.add(new InsertBatchSomeColumn());
      methods

    }
  }

}
