package org.apache.kyuubi.lineage.db.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import java.util.List;

/**
 * @author thomasgx
 * @date 2023年03月13日 18:45
 */
public interface SqlBaseMapper<T> extends BaseMapper<T> {
  Integer insertBatchSomeColumn(List<T> entityList);
}
