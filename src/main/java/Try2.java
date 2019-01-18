import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Try2 {
    public static void main(String args[]) {
        String fileName = args[0];

        SparkSession spark = new SparkSession.Builder()
                .appName("Try1")
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> df = spark.read()
                .schema("id1 int, id2 int")
                .csv(fileName);

        List<Column> groupByCols = new ArrayList<>();
        groupByCols.add(new Column("id1"));
        scala.collection.Seq<Column> groupByColsSeq =
                JavaConverters.asScalaIteratorConverter(groupByCols.iterator()).asScala().toSeq();
        df.groupBy(groupByColsSeq).max("id2").toDF("id1", "id2").show();
    }
}
