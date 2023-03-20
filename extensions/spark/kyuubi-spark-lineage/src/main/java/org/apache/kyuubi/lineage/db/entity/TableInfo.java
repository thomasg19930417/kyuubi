package org.apache.kyuubi.lineage.db.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import java.util.List;

/**
 * @author thomasgx
 * @date 2023年03月03日 19:04
 */
@TableName("dgm_table_info")
public class TableInfo {
  @TableId(value = "id", type = IdType.AUTO)
  private Long id;

  @TableField("ds_id")
  private Long dsId;

  @TableField("database_name")
  private String databaseName;

  @TableField("table_ename")
  private String tableEname;

  @TableField("table_cname")
  private String tableCname;

  @TableField("table_owner")
  private String tableOwner;

  @TableField("create_time")
  private String createTime;

  @TableField("update_time")
  private String updateTime;

  @TableField("deleted")
  private Integer deleted;

  @TableField("is_partition")
  private Boolean isPartition;

  @TableField("storage_capacity")
  private String storageCapacity;

  @TableField("data_num")
  private String dataNum;

  @TableField("description")
  private String description;

  @TableField("cu")
  private String cu;

  @TableField("mu")
  private String mu;

  @TableField(exist = false)
  private List<TableColumnInfo> tableColumnInfoList;

  public TableInfo() {}

  public TableInfo(
      Long id,
      Long dsId,
      String databaseName,
      String tableEname,
      String tableCname,
      String tableOwner,
      String createTime,
      String updateTime,
      Integer deleted,
      Boolean isPartition,
      String storageCapacity,
      String dataNum,
      String description,
      List<TableColumnInfo> tableColumnInfoList) {
    this.id = id;
    this.dsId = dsId;
    this.databaseName = databaseName;
    this.tableEname = tableEname;
    this.tableCname = tableCname;
    this.tableOwner = tableOwner;
    this.createTime = createTime;
    this.updateTime = updateTime;
    this.deleted = deleted;
    this.isPartition = isPartition;
    this.storageCapacity = storageCapacity;
    this.dataNum = dataNum;
    this.description = description;
    this.tableColumnInfoList = tableColumnInfoList;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public Long getDsId() {
    return dsId;
  }

  public void setDsId(Long dsId) {
    this.dsId = dsId;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableEname() {
    return tableEname;
  }

  public void setTableEname(String tableEname) {
    this.tableEname = tableEname;
  }

  public String getTableCname() {
    return tableCname;
  }

  public void setTableCname(String tableCname) {
    this.tableCname = tableCname;
  }

  public String getTableOwner() {
    return tableOwner;
  }

  public void setTableOwner(String tableOwner) {
    this.tableOwner = tableOwner;
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

  public Boolean getPartition() {
    return isPartition;
  }

  public void setPartition(Boolean partition) {
    isPartition = partition;
  }

  public String getStorageCapacity() {
    return storageCapacity;
  }

  public void setStorageCapacity(String storageCapacity) {
    this.storageCapacity = storageCapacity;
  }

  public String getDataNum() {
    return dataNum;
  }

  public void setDataNum(String dataNum) {
    this.dataNum = dataNum;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getCu() {
    return cu;
  }

  public void setCu(String cu) {
    this.cu = cu;
  }

  public String getMu() {
    return mu;
  }

  public void setMu(String mu) {
    this.mu = mu;
  }

  public List<TableColumnInfo> getTableColumnInfoList() {
    return tableColumnInfoList;
  }

  public void setTableColumnInfoList(List<TableColumnInfo> tableColumnInfoList) {
    this.tableColumnInfoList = tableColumnInfoList;
  }

  @Override
  public String toString() {
    return "TableInfo{"
        + "id="
        + id
        + ", dsId="
        + dsId
        + ", databaseName='"
        + databaseName
        + '\''
        + ", tableEname='"
        + tableEname
        + '\''
        + ", tableCname='"
        + tableCname
        + '\''
        + ", tableOwner='"
        + tableOwner
        + '\''
        + ", createTime='"
        + createTime
        + '\''
        + ", updateTime='"
        + updateTime
        + '\''
        + ", deleted="
        + deleted
        + ", isPartition="
        + isPartition
        + ", storageCapacity='"
        + storageCapacity
        + '\''
        + ", dataNum='"
        + dataNum
        + '\''
        + ", description='"
        + description
        + '\''
        + ", cu='"
        + cu
        + '\''
        + ", mu='"
        + mu
        + '\''
        + '}';
  }

  public static class TableInfoBuilder {
    private Long id;
    private Long dsId;
    private String databaseName;
    private String tableEname;
    private String tableCname;
    private String tableOwner;
    private String createTime;
    private String updateTime;
    private Integer deleted;
    private Boolean isPartition;
    private String storageCapacity;
    private String dataNum;
    private String description;
    private List<TableColumnInfo> tableColumnInfoList;

    public TableInfoBuilder setId(Long id) {
      this.id = id;
      return this;
    }

    public TableInfoBuilder setDsId(Long dsId) {
      this.dsId = dsId;
      return this;
    }

    public TableInfoBuilder setDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    public TableInfoBuilder setTableEname(String tableEname) {
      this.tableEname = tableEname;
      return this;
    }

    public TableInfoBuilder setTableCname(String tableCname) {
      this.tableCname = tableCname;
      return this;
    }

    public TableInfoBuilder setTableOwner(String tableOwner) {
      this.tableOwner = tableOwner;
      return this;
    }

    public TableInfoBuilder setCreateTime(String createTime) {
      this.createTime = createTime;
      return this;
    }

    public TableInfoBuilder setUpdateTime(String updateTime) {
      this.updateTime = updateTime;
      return this;
    }

    public TableInfoBuilder setDeleted(Integer deleted) {
      this.deleted = deleted;
      return this;
    }

    public TableInfoBuilder setPartition(Boolean partition) {
      isPartition = partition;
      return this;
    }

    public TableInfoBuilder setStorageCapacity(String storageCapacity) {
      this.storageCapacity = storageCapacity;
      return this;
    }

    public TableInfoBuilder setDataNum(String dataNum) {
      this.dataNum = dataNum;
      return this;
    }

    public TableInfoBuilder setDescription(String description) {
      this.description = description;
      return this;
    }

    public TableInfoBuilder setTableColumnInfoList(List<TableColumnInfo> tableColumnInfoList) {
      this.tableColumnInfoList = tableColumnInfoList;
      return this;
    }

    public TableInfo builder() {
      return new TableInfo(
          id,
          dsId,
          databaseName,
          tableEname,
          tableCname,
          tableOwner,
          createTime,
          updateTime,
          deleted,
          isPartition,
          storageCapacity,
          dataNum,
          description,
          tableColumnInfoList);
    }
  }
}
