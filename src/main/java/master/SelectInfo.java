package master; // 确保在 master 包下

import java.util.Objects;

public class SelectInfo {
    public boolean isValid = false; // 改为 public 方便 client.Client 直接访问，或者提供 getter
    public String ip = null;       // 改为 public
    public String sql = null;        // 改为 public

    public SelectInfo(boolean isValid, String ip, String sql) {
        this.isValid = isValid;
        if (this.isValid) {
            this.ip = ip; // ip 将存储 "ip:port"
            this.sql = sql;
        }
    }

    public SelectInfo(boolean isValid) {
        this.isValid = isValid;
    }

    // 用户 Client 调用这个构造函数，从 Master 返回的字符串解析信息
    public SelectInfo(String str) {
        if (str == null || str.isEmpty()) {
            this.isValid = false;
            System.err.println("[SelectInfo] Error: Received null or empty string to parse.");
            return;
        }
        // 分割时，限制分割次数为3，确保SQL语句中的空格不会导致问题
        String[] tokens = str.split(" ", 3);
        if (tokens.length > 0 && "true".equals(tokens[0])) { // 使用 "true".equals 避免 NullPointerException
            if (tokens.length == 3) {
                this.isValid = true;
                this.ip = tokens[1]; // ip 应该是 "ip:port"
                this.sql = tokens[2];
            } else {
                System.err.println("[SelectInfo] Error: 'true' status but incorrect number of tokens. Expected 3, got " + tokens.length + ". String: " + str);
                this.isValid = false;
            }
        } else if (tokens.length > 0 && "false".equals(tokens[0])) {
            this.isValid = false;
        } else {
            System.err.println("[SelectInfo] Error: Cannot parse SelectInfo string. Unknown format: " + str);
            this.isValid = false;
        }
    }

    public String Serialize() {
        String ret = "";
        if (this.isValid) {
            // 确保 ip 和 sql 不为 null，尽管构造函数应该处理了
            ret += "true " + (this.ip != null ? this.ip : "null_ip") + " " + (this.sql != null ? this.sql : "null_sql");
        } else {
            ret += "false";
        }
        return ret;
    }

    // 可选的 getter 方法，如果不想把字段设为 public
    // public boolean isValid() { return isValid; }
    // public String getIp() { return ip; }
    // public String getSql() { return sql; }
}