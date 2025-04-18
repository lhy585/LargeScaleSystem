package socket;

public enum SqlType {
    SELECT,   // 查询
    INSERT,   // 插入
    UPDATE,   // 更新
    DELETE,   // 删除
    CREATE,   // 创建表
    DROP,     // 删除表
    ALTER,    // 修改表
    TRUNCATE, // 清空表
    UNKNOWN   // 未知类型
}