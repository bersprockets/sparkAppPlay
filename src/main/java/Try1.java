import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Iterator;

public class Try1 {
    public static void main(String args[]) {
        SparkSession spark = new SparkSession.Builder()
                .appName("Try1")
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .table("orctblthin")
                .filter("id1 = 1 and id2 is not null");
        Iterator<Row> it = df.toLocalIterator();
        for (int i = 0; i < 20 && it.hasNext(); i++) {
            System.out.println(it.next().getDouble(1));
        }
    }
}
