package org.cloud.data;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SparkHudiReadDataOnehouse {
    public static void main(String[] args) {
        System.out.println("Hello World");
        System.setProperty("hadoop.home.dir", "D:\\sparksetup\\hadoop");
        System.setProperty("java.library.path","D:\\sparksetup\\hadoop\\bin");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Example Spark App")
                .setMaster("local[*]")
                .set("className", "org.apache.hudi")
                .set("spark.sql.hive.convertMetastoreParquet", "false")
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .set("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .set("spark.kryo.registrator","org.apache.spark.HoodieSparkKryoRegistrar")
                .set("spark.sql.warehouse.dir", "file:///D:/sparksetup/iceberg/spark_warehouse");
        SparkSession spark = SparkSession.builder().appName("Spark Hudi Read").config(sparkConf).getOrCreate();

        //Read table data
        Dataset trips_df = spark.read().format("org.apache.hudi").load("file:///D:/sparksetup/iceberg/spark_warehouse/uber/trips/data");

        trips_df.show();


        //trips_df.createOrReplaceTempView("trips_table");
        //spark.sql("SELECT * FROM  trips_table").show(false);
    }
}