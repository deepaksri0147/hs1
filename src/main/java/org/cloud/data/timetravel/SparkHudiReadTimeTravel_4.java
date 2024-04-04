package org.cloud.data.timetravel;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


public class SparkHudiReadTimeTravel_4 {
    public static void main(String[] args) {
        System.out.println("Hello World");


        String storageAccountName = "test1datalakestoragegen2";
        String storageAccountKey = "3K/fWSAN7/PJNPr7qBcH5idJfu8W96ISGXpbg8+i2vAYWR+D1LKZhVeEyuIIhEaXiOjXt9Osc9py+AStV4xdQw==";


        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("className", "org.apache.hudi")
                .set("spark.sql.hive.convertMetastoreParquet", "false")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
                .set("spark.sql.warehouse.dir", "/home/gaian/spark-warehouse")
                .set("fs.defaultFS", "abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/")
                .set("fs.azure.account.key." + storageAccountName + ".dfs.core.windows.net", storageAccountKey);
        SparkSession spark = SparkSession.builder().appName("Spark Hudi Read").config(sparkConf).getOrCreate();

        //Read table data before update
        Dataset trips_df = spark.read().format("org.apache.hudi").option("as.of.instant","20240221123839229").load("abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/Spark_04");
        trips_df.createOrReplaceTempView("Spark_04");
        spark.sql("SELECT * FROM  Spark_04").show();

        //Read table data after update
//        Dataset trips_df2 = spark.read().format("org.apache.hudi").load("abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/Flink_31");
//        trips_df2.createOrReplaceTempView("Flink_31");
//        spark.sql("SELECT *  FROM  Flink_31").show();
    }
}
