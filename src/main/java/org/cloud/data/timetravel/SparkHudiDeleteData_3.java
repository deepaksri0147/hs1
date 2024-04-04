package org.cloud.data.timetravel;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;


public class SparkHudiDeleteData_3 {
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

        //Read table data
        Dataset trips_df = spark.read().format("org.apache.hudi")
                .load("abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/Spark_05");

        Dataset trips_df_updates =  trips_df.filter(trips_df.col("rider").equalTo("piyush-F"));
        trips_df_updates.write().format("org.apache.hudi").option("hoodie.datasource.write.operation","delete")
                .option(PARTITIONPATH_FIELD_NAME.key(),"uuid").option("hoodie.table.name","Spark_05").mode(SaveMode.Append)
                .save("abfs://hudi@test1datalakestoragegen2.dfs.core.windows.net/hudi/deepak/Spark_05");
    }
}
