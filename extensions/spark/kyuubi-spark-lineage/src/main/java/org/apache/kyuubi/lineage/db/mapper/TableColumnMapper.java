package org.apache.kyuubi.lineage.db.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;
import org.apache.kyuubi.lineage.db.entity.TableColumnInfo;

/**
 * @author thomasgx
 * @date 2023年03月08日 10:21
 */
@Mapper
public interface TableColumnMapper extends SqlBaseMapper<TableColumnInfo> {

  @Update(
      "update dgm_field_info set deleted=1 , field_ename=concat(field_ename,#{flag}) where table_id=${tableId}")
  Integer deleteFieldByTableId(@Param("tableId") Long tableId, @Param("flag") String flag);

  @Select(
      "select count(*) cnt from "
          + "dgm_table_info a "
          + "inner join "
          + "dgm_field_info b "
          + "on "
          + "a.id = b.table_id "
          + "where "
          + " a.ds_id=#{dsId}"
          + " and a.database_name=#{dbName}"
          + " and a.table_ename=#{tbName}"
          + " and a.deleted=0 and b.deleted=0")
  Integer getTableColumnsCount(
      @Param("dsId") Integer dsId, @Param("dbName") String dbName, @Param("tbName") String tbName);
}
