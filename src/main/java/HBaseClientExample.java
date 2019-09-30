
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClientExample {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        String path = HBaseClientExample.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(path));

        HBaseAdmin.checkHBaseAvailable(config);

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            TableName table1 = TableName.valueOf("player");
            Table table = connection.getTable(table1);
            Scan scan = new Scan();
            String family1 = "cf";
            scan.addFamily(family1.getBytes());

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                String playerName = Bytes.toString(result.getRow());
                String playerId = null;
                String teamId = null;
                String height = null;
                for (Cell cell : result.rawCells()) {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    if (qualifier.equals("player_id")) {
                        playerId = Bytes.toString(CellUtil.cloneValue(cell));
                    } else if (qualifier.equals("team_id")) {
                        teamId = Bytes.toString(CellUtil.cloneValue(cell));
                    } else {
                        height = Bytes.toString(CellUtil.cloneValue(cell));
                    }
                }
                System.out.println(String.format("%s -> %s | %s | %s", playerName, playerId, teamId, height));
            }
        }

    }
}
