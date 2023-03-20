package org.apache.kyuubi.lineage.db.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import java.util.List;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;
import org.apache.kyuubi.lineage.db.entity.TableLineageInfo;

/**
 * @author thomasgx
 * @date 2023年03月13日 15:13
 */
@Mapper
public interface TableLineageInfoMapper extends BaseMapper<TableLineageInfo> {
  Integer insertBatchSomeColumn(List<TableLineageInfo> entityList);

  @Update(
      "update dgm_table_lineage set deleted=1 where source_id=#{deleteId} or target_id=#{deleteId}")
  void logicalDeletedLineage(@Param("deleteId") Long id);

  @Insert({
    "<script>",
    "insert into dgm_table_lineage(source_id,target_id,create_time,modify_time) values",
    "<foreach collection='entityList' item='item'  separator=','>",
    "(#{item.sourceId},#{item.targetId},#{item.createTime},#{item.modifyTime})",
    "</foreach>",
    "on duplicate key update modify_time=values(modify_time)",
    "</script>"
  })
  Integer saveOrUpdate(@Param("entityList") List<TableLineageInfo> entityList);
}
