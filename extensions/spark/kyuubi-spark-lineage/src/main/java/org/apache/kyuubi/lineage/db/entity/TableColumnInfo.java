package org.apache.kyuubi.lineage.db.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

/**
 * @author thomasgx
 * @date 2023年03月06日 14:19
 */
@TableName("dgm_field_info")
public class TableColumnInfo {
  @TableId(value = "id", type = IdType.AUTO)
  private Long id;

  @TableField("table_id")
  private Long tableId;

  @TableField("field_ename")
  private String fieldEname;

  @TableField("field_cname")
  private String fieldCname;

  @TableField("field_type")
  private String fieldType;

  @TableField("`index`")
  private Integer index;

  @TableField("is_partition")
  private Integer isPartition;

  @TableField("create_time")
  private String createTime;

  @TableField("update_time")
  private String updateTime;

  @TableField("deleted")
  private Integer deleted;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getTableId() {
    return tableId;
  }

  public void setTableId(Long tableId) {
    this.tableId = tableId;
  }

  public String getFieldEname() {
    return fieldEname;
  }

  public void setFieldEname(String fieldEname) {
    this.fieldEname = fieldEname;
  }

  public String getFieldCname() {
    return fieldCname;
  }

  public void setFieldCname(String fieldCname) {
    this.fieldCname = fieldCname;
  }

  public String getFieldType() {
    return fieldType;
  }

  public void setFieldType(String fieldType) {
    this.fieldType = fieldType;
  }

  public Integer getIndex() {
    return index;
  }

  public void setIndex(Integer index) {
    this.index = index;
  }

  public Integer getPartition() {
    return isPartition;
  }

  public void setPartition(Integer partition) {
    isPartition = partition;
  }

  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }

  public Integer getDeleted() {
    return deleted;
  }

  public void setDeleted(Integer deleted) {
    this.deleted = deleted;
  }

  @Override
  public String toString() {
    return "BaseColumnInfo{"
        + "id="
        + id
        + ", tableId="
        + tableId
        + ", fieldEname='"
        + fieldEname
        + '\''
        + ", fieldCname='"
        + fieldCname
        + '\''
        + ", fieldType='"
        + fieldType
        + '\''
        + ", index="
        + index
        + ", isPartition="
        + isPartition
        + ", createTime='"
        + createTime
        + '\''
        + ", updateTime='"
        + updateTime
        + '\''
        + ", deleted="
        + deleted
        + '}';
  }
}
