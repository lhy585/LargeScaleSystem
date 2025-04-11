package socket;

import lombok.Getter;
import net.sf.jsqlparser.statement.*;

@Getter
public class ParsedSqlResult {
    public ParsedSqlResult(SqlType type, String tableName, Statement statement) {
        this.type = type;
        this.tableName = tableName;
        this.statement = statement;
    }

    @Override
    public String toString() {
        return "Statement = " + statement +".";
    }

    @Getter
    public final SqlType type;
    @Getter
    public String tableName;
    @Getter
    public final Statement statement;
}
