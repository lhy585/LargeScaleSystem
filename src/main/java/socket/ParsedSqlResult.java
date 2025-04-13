package socket;

import lombok.Getter;
import net.sf.jsqlparser.statement.*;

import java.util.List;

@Getter
public class ParsedSqlResult {
    public ParsedSqlResult(SqlType type, List<String> tableNames, Statement statement) {
        this.type = type;
        this.tableNames = tableNames;
        this.statement = statement;
    }

    @Override
    public String toString() {
        return "Statement = " + statement +".";
    }

    @Getter
    public final SqlType type;
    @Getter
    public List<String> tableNames;
    @Getter
    public final Statement statement;
}