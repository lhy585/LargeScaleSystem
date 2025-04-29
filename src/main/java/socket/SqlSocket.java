package socket;

import lombok.Getter;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

@Getter
public class SqlSocket {

    public SqlSocket(Socket newSocket) {
        try {
            socket = newSocket;
            input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            output = new PrintWriter(socket.getOutputStream(), true);
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void destroy() throws IOException {
        input.close();
        output.close();
        socket.close();
    }

    @Deprecated
    /*
      测试专用，传入SQL语句，将SQL语句分词结果打印在控制台
     */
    public SqlSocket(String sql){
        this.output = new PrintWriter(System.out, true); //绑定控制台输出，避免空指针
        parseSql(sql);
    }

    /**
     * 解析客户端传入的SQL语句，构造ParsedSqlResult类，其中存储了SQL操作名与表名
     */
    public void parseSql(String sql) {
        try {
            if (sql.contains("REGISTER"))return;
            System.out.println("Request is: \" " + sql + " \"");
            Statement statement = CCJSqlParserUtil.parse(sql);
            SqlType type;
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tableNames;

            if (statement instanceof Select) {
                type = SqlType.SELECT;
                Select select = (Select) statement;
                PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

                output.println("✅ 发送的是 SELECT 语句");
                output.println("📌 查询字段：");
                for (SelectItem item : plainSelect.getSelectItems()) {
                    output.println("  - " + item);
                }
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 查询表名：" + tableNames);
                if (plainSelect.getWhere() != null) {
                    output.println("📌 查询条件：" + plainSelect.getWhere());
                }
            } else if (statement instanceof Insert) {
                type = SqlType.INSERT;
                Insert insert = (Insert) statement;

                output.println("✅ 发送的是 INSERT 语句");
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 插入表名：" + tableNames);
                output.println("📌 字段列表：" + insert.getColumns());
                output.println("📌 插入值：" + insert.getItemsList());
            } else if (statement instanceof CreateTable) {
                type = SqlType.CREATE;
                CreateTable create = (CreateTable) statement;

                output.println("✅ 发送的是 CREATE TABLE 语句");
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 表名：" + tableNames);
                output.println("📌 字段定义：");
                create.getColumnDefinitions().forEach(col -> output.println("  - " + col));
            } else if (statement instanceof Delete) {
                type = SqlType.DELETE;
                Delete delete = (Delete) statement;

                output.println("✅ 发送的是 DELETE 语句");
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 删除表名：" + tableNames);
                if (delete.getWhere() != null) {
                    output.println("📌 删除条件：" + delete.getWhere());
                }
            } else if (statement instanceof Update) {
                type = SqlType.UPDATE;
                Update update = (Update) statement;

                output.println("✅ 发送的是 UPDATE 语句");
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 更新表名：" + tableNames);
                output.println("📌 更新字段：");
                for (int i = 0; i < update.getColumns().size(); i++) {
                    output.println("  - " + update.getColumns().get(i) + " = " + update.getExpressions().get(i));
                }
                if (update.getWhere() != null) {
                    output.println("📌 更新条件：" + update.getWhere());
                }
            } else if (statement instanceof Alter) {
                type = SqlType.ALTER;
                Alter alter = (Alter) statement;

                output.println("✅ 发送的是 ALTER 语句");
                String tableName = alter.getTable().getName();
                tableNames = Collections.singletonList(tableName);
                output.println("📌 修改的表名：" + tableNames);
                output.println("📌 修改操作列表：");
                alter.getAlterExpressions().forEach(expr -> output.println("  - " + expr));
            } else if (statement instanceof Truncate) {
                type = SqlType.TRUNCATE;
                Truncate truncate = (Truncate) statement;

                output.println("✅ 发送的是 TRUNCATE 语句");
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 清空的表名：" + tableNames);
            }else if (statement instanceof Drop) {
                type = SqlType.DROP;
                Drop drop = (Drop) statement;

                output.println("✅ 发送的是 DROP 语句");
                output.println("📌 删除类型：" + drop.getType());
                tableNames = tablesNamesFinder.getTableList(statement);
                output.println("📌 删除的表名：" + tableNames);
            } else {
                type = SqlType.UNKNOWN;
                tableNames = null;
                output.println("⚠️ 暂不支持解析的语句类型：" + statement.getClass().getSimpleName());
            }
            parsedSqlResult = new ParsedSqlResult(type, tableNames, statement);
        } catch (Exception e) {
            output.println("❌ SQL 解析失败：" + e.getMessage());
            e.printStackTrace();
        }
    }

    private Socket socket;
    private BufferedReader input;
    private PrintWriter output;
    private ParsedSqlResult parsedSqlResult;
}