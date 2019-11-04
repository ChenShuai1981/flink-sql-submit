import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class CreateViewExample {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);
        tEnv.sqlUpdate("CREATE VIEW x AS SELECT 1+1");
        Table tbl = tEnv.sqlQuery("SELECT * FROM x");
        tbl.printSchema();

        tEnv.execute("CreateViewExample");
    }

}
