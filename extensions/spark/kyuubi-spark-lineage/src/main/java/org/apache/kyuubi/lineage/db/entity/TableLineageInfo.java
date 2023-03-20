package org.apache.kyuubi.lineage.db.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * @author thomasgx
 * @date 2023年03月06日 10:12
 */
@TableName("dgm_table_lineage")
public class TableLineageInfo {
  @TableId(value = "id", type = IdType.AUTO)
  private Long id;

  @TableField("source_id")
  private Long sourceId;

  @TableField("target_id")
  private Long targetId;

  @TableField("create_time")
  private String createTime;

  @TableField("modify_time")
  private String modifyTime;

  @TableField("deleted")
  private Integer deleted = 0;

  public TableLineageInfo() {}

  public TableLineageInfo(Long sourceId, Long targetId, String createTime, String modifyTime) {
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.createTime = createTime;
    this.modifyTime = modifyTime;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getSourceId() {
    return sourceId;
  }

  public void setSourceId(Long sourceId) {
    this.sourceId = sourceId;
  }

  public Long getTargetId() {
    return targetId;
  }

  public void setTargetId(Long targetId) {
    this.targetId = targetId;
  }

  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public String getModifyTime() {
    return modifyTime;
  }

  public void setModifyTime(String modifyTime) {
    this.modifyTime = modifyTime;
  }

  public Integer getDeleted() {
    return deleted;
  }

  public void setDeleted(Integer deleted) {
    this.deleted = deleted;
  }

  @Override
  public String toString() {
    return "TableLineageInfo{"
        + "id="
        + id
        + ", sourceId="
        + sourceId
        + ", targetId="
        + targetId
        + ", createTime='"
        + createTime
        + '\''
        + ", modifyTime='"
        + modifyTime
        + '\''
        + ", deleted="
        + deleted
        + '}';
  }
}
