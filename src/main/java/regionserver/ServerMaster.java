package regionserver;

import zookeeper.ZooKeeperManager; // ServerMaster.migrateTable 会用到它来获取源信息

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.charset.StandardCharsets;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.concurrent.TimeUnit;

/**
 * 包含由 RegionServer 执行的、与数据表物理迁移/复制相关的静态方法。
 * 通常由 Master 通过命令间接调用。
 */
public class ServerMaster {

    private static final String ZK_REGION_SERVER_BASE_PATH = "/lss/region_server"; // ZK中RegionServer的父路径
    private static final String TARGET_DB_NAME = "lss"; // 目标数据库名

    /**
     * 将表从源 RegionServer 复制(转储)到当前 RegionServer (作为目标)。
     * 此方法在 *目标* RegionServer 上被调用执行。
     *
     * @param sourceRegionNameForLog 源服务器的标识 (例如 IP 地址, 用于日志)。
     * @param sourceMysqlHost        源 MySQL 实例的主机名/IP。
     * @param sourceMysqlPort        源 MySQL 实例的端口。
     * @param sourceMysqlUser        源 MySQL 的用户名。
     * @param sourceMysqlPwd         源 MySQL 的密码。
     * @param tableNameOnSource      要从源转储的表名。
     * @param targetMysqlUser        目标 (本地) MySQL 的用户名 (即当前RegionServer的MySQL用户)。
     * @param targetMysqlPwd         目标 (本地) MySQL 的密码 (即当前RegionServer的MySQL密码)。
     * @return 如果转储和导入成功，则返回 true，否则返回 false。
     */
    public static boolean dumpTable(String sourceRegionNameForLog, String sourceMysqlHost, String sourceMysqlPort,
                                    String sourceMysqlUser, String sourceMysqlPwd, String tableNameOnSource,
                                    String targetMysqlUser, String targetMysqlPwd) {
        System.out.println("[ServerMaster@"+RegionServer.ip+"] 开始复制表 '" + tableNameOnSource + "' 从 " + sourceRegionNameForLog + " (" + sourceMysqlHost + ":" + sourceMysqlPort + ")");

        File tempDir = new File(System.getProperty("java.io.tmpdir"));
        if (!tempDir.exists() && !tempDir.mkdirs()) {
            System.err.println("[ServerMaster@"+RegionServer.ip+"] 无法创建临时目录: " + tempDir.getAbsolutePath());
            return false;
        }
        File dumpFile = new File(tempDir, "dump_" + tableNameOnSource + "_" + System.currentTimeMillis() + ".sql");
        String absoluteDumpFilePath = dumpFile.getAbsolutePath();
        System.out.println("[ServerMaster@"+RegionServer.ip+"] 使用临时转储文件: " + absoluteDumpFilePath);

        Process dumpProcess = null;
        Process importProcess = null;

        try {
            String osName = System.getProperty("os.name").toLowerCase();

            // 1. 从源 MySQL 使用 mysqldump 转储表数据
            String dumpCmdStr = String.format(
                    "mysqldump --single-transaction --skip-tz-utc -h %s -P %s -u %s -p%s --databases %s --tables %s --result-file=\"%s\"",
                    sourceMysqlHost, sourceMysqlPort, sourceMysqlUser, sourceMysqlPwd, TARGET_DB_NAME, tableNameOnSource, absoluteDumpFilePath
            );
            System.out.println("[ServerMaster@"+RegionServer.ip+"] 执行转储命令 (密码已屏蔽): " + dumpCmdStr.replace("-p"+sourceMysqlPwd, "-p****"));
            dumpProcess = Runtime.getRuntime().exec(dumpCmdStr);

            readStream(dumpProcess.getErrorStream(), "DumpErr@" + RegionServer.ip);
            readStream(dumpProcess.getInputStream(), "DumpOut@" + RegionServer.ip);

            if (!dumpProcess.waitFor(120, TimeUnit.SECONDS)) { // 例如，等待120秒
                System.err.println("[ServerMaster@"+RegionServer.ip+"] mysqldump 执行超时。");
                dumpProcess.destroyForcibly();
                return false;
            }
            int dumpExitCode = dumpProcess.exitValue();

            if (dumpExitCode != 0) {
                System.err.println("[ServerMaster@"+RegionServer.ip+"] mysqldump 执行失败。退出码: " + dumpExitCode + ". 详情请查看 DumpErr 输出。");
                return false;
            }
            if (!dumpFile.exists() || dumpFile.length() == 0) {
                System.err.println("[ServerMaster@"+RegionServer.ip+"] 转储文件未创建或为空: " + absoluteDumpFilePath + " (可能是mysqldump内部错误或权限问题)");
                return false;
            }
            System.out.println("[ServerMaster@"+RegionServer.ip+"] mysqldump 成功输出到文件: " + absoluteDumpFilePath + " (大小: " + dumpFile.length() + " 字节)");

            // 2. 将转储文件导入到目标 (本地) MySQL
            // RegionServer.mysqlPort 是当前RegionServer的MySQL端口
            String importCmdStr = String.format(
                    "mysql -h localhost -P %s -u %s -p%s < \"%s\"",
                    RegionServer.mysqlPort, targetMysqlUser, targetMysqlPwd, absoluteDumpFilePath
            );
            System.out.println("[ServerMaster@"+RegionServer.ip+"] 执行导入命令 (密码已屏蔽): " + importCmdStr.replace("-p"+targetMysqlPwd, "-p****"));

            if (osName.contains("win")) {
                importProcess = Runtime.getRuntime().exec(new String[]{"cmd", "/c", importCmdStr});
            } else {
                importProcess = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", importCmdStr});
            }

            readStream(importProcess.getErrorStream(), "ImportErr@" + RegionServer.ip);
            readStream(importProcess.getInputStream(), "ImportOut@" + RegionServer.ip);

            if (!importProcess.waitFor(120, TimeUnit.SECONDS)) { // 例如，等待120秒
                System.err.println("[ServerMaster@"+RegionServer.ip+"] mysql 导入执行超时。");
                importProcess.destroyForcibly();
                return false;
            }
            int importExitCode = importProcess.exitValue();

            if (importExitCode != 0) {
                System.err.println("[ServerMaster@"+RegionServer.ip+"] mysql 导入失败。退出码: " + importExitCode + ". 详情请查看 ImportErr 输出。");
                return false;
            }
            System.out.println("[ServerMaster@"+RegionServer.ip+"] mysql 成功导入表: " + tableNameOnSource + " (作为 " + tableNameOnSource + " 导入)。");
            return true;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[ServerMaster@"+RegionServer.ip+"] 表 '" + tableNameOnSource + "' 的转储/导入过程被中断: " + e.getMessage());
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            System.err.println("[ServerMaster@"+RegionServer.ip+"] 表 '" + tableNameOnSource + "' 的转储/导入过程中发生 IOException: " + e.getMessage());
            e.printStackTrace();
            return false;
        } finally {
            if (dumpProcess != null && dumpProcess.isAlive()) dumpProcess.destroyForcibly();
            if (importProcess != null && importProcess.isAlive()) importProcess.destroyForcibly();

            if (dumpFile.exists()) {
                if (!dumpFile.delete()) {
                    System.err.println("[ServerMaster@"+RegionServer.ip+"] 警告: 删除临时转储文件失败: " + absoluteDumpFilePath);
                } else {
                    // System.out.println("[ServerMaster@"+RegionServer.ip+"] 已删除临时转储文件: " + absoluteDumpFilePath);
                }
            }
        }
    }

    /**
     * (此方法在RegionServer端执行，由Master通过命令触发)
     * 通过从源转储、导入到本RS、并从源RS的数据库删除的方式迁移表。
     * 注意：ZK元数据更新由Master负责。此方法只负责数据层面迁移。
     *
     * @param sourceRegionIp   源 RegionServer 的 IP (用于连接其MySQL)。
     * @param sourceMysqlPort  源 MySQL 实例的端口。
     * @param sourceMysqlUser  源 MySQL 的用户名。
     * @param sourceMysqlPwd   源 MySQL 的密码。
     * @param tableName        要迁移的表名 (在源和目标上名称相同)。
     * @param localTargetUser  当前RegionServer (目标) 的MySQL用户名。
     * @param localTargetPwd   当前RegionServer (目标) 的MySQL密码。
     * @return 如果数据复制和源数据删除都成功，则返回 true。
     */
    public static boolean executeCompleteMigrationSteps(String sourceRegionIp, String sourceMysqlPort,
                                                        String sourceMysqlUser, String sourceMysqlPwd,
                                                        String tableName,
                                                        String localTargetUser, String localTargetPwd) {
        System.out.println("[ServerMaster@"+RegionServer.ip+"] 开始执行完整迁移步骤，表: " + tableName + " 从源: " + sourceRegionIp);

        // 步骤 1: 从源复制数据到本RS (ServerMaster.dumpTable做这个)
        boolean dumpImportSuccess = dumpTable(
                sourceRegionIp, // sourceRegionNameForLog
                sourceRegionIp, sourceMysqlPort, sourceMysqlUser, sourceMysqlPwd, // source DB details
                tableName,    // tableNameOnSource
                localTargetUser, localTargetPwd // target (local) MySQL credentials
        );

        if (!dumpImportSuccess) {
            System.err.println("[ServerMaster@"+RegionServer.ip+"] 完整迁移失败：数据复制步骤 (dumpTable) 失败，表: " + tableName);
            return false;
        }
        System.out.println("[ServerMaster@"+RegionServer.ip+"] 完整迁移：数据复制成功，表: " + tableName);

        // 步骤 2: 从源 RegionServer 的 MySQL 中删除表
        System.out.println("[ServerMaster@"+RegionServer.ip+"] 完整迁移：尝试连接源 MySQL (" + sourceRegionIp + ":" + sourceMysqlPort + ") 删除表 '" + tableName + "'...");
        Connection sourceConn = null;
        Statement sourceStmt = null;
        try {
            String sourceJdbcIpParam = sourceMysqlPort.equals("3306") ? sourceRegionIp : (sourceRegionIp + ":" + sourceMysqlPort);
            sourceConn = JdbcUtils.getConnection(sourceJdbcIpParam, sourceMysqlUser, sourceMysqlPwd);

            if (sourceConn != null) {
                sourceStmt = sourceConn.createStatement();
                sourceStmt.execute("USE " + TARGET_DB_NAME);
                String dropSql = "DROP TABLE IF EXISTS `" + tableName + "`";
                System.out.println("[ServerMaster@"+RegionServer.ip+"] 在源 (" + sourceRegionIp + ") 上执行: " + dropSql);
                sourceStmt.executeUpdate(dropSql);
                System.out.println("[ServerMaster@"+RegionServer.ip+"] 表 '" + tableName + "' 已成功从源 " + sourceRegionIp + " 的数据库中删除。");
                return true; // 整个迁移步骤成功
            } else {
                System.err.println("[ServerMaster@"+RegionServer.ip+"] 完整迁移警告：无法连接到源 MySQL ("+ sourceRegionIp + ":" + sourceMysqlPort +") 来删除表 '" + tableName + "'。数据已复制，但源表未删除。");
                return false; // 标记为不完全成功
            }
        } catch (SQLException e) {
            System.err.println("[ServerMaster@"+RegionServer.ip+"] 完整迁移错误：从源 MySQL 删除表 '" + tableName + "' 时出错: " + e.getMessage());
            e.printStackTrace();
            return false; // 标记为不完全成功
        } finally {
            JdbcUtils.releaseResc(null, sourceStmt, sourceConn);
        }
    }


    private static void readStream(InputStream stream, String streamName) {
        new Thread(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println("[" + streamName + "] " + line);
                }
            } catch (IOException e) {
                System.err.println("读取流 " + streamName + " 时出错: " + e.getMessage());
            }
        }, streamName + "-Reader-" + Thread.currentThread().getId()).start();
    }
}