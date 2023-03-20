package org.apache.kyuubi.lineage.db.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.kyuubi.lineage.db.entity.TableInfo;

/**
 * @author thomasgx
 * @date 2023年03月08日 9:39
 */
@Mapper
public interface TableInfoMapper extends BaseMapper<TableInfo> {

  @Select(
      "select id from  dgm_table_info "
          + "where ds_id=#{dsId}"
          + " and database_name=#{dbName}"
          + " and table_ename=#{tbName}")
  Long getTableId(
      @Param("dsId") Integer dsId, @Param("dbName") String dbName, @Param("tbName") String tbName);
}
