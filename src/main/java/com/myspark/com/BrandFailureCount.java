package com.myspark.com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.Map;

public class BrandFailureCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Brand Failure Count")
                .master("local[*]")  // 使用local模式，实际运行时应根据集群环境设置
                .getOrCreate();

        // 定义品牌前缀映射
        Map<String, String> brandPrefixMap = new HashMap<>();
        brandPrefixMap.put("CT", "Crucial");
        brandPrefixMap.put("DELLBOSS", "Dell BOSS");
        brandPrefixMap.put("HGST", "HGST");
        brandPrefixMap.put("Seagate", "Seagate");
        brandPrefixMap.put("ST", "Seagate");
        brandPrefixMap.put("TOSHIBA", "Toshiba");
        brandPrefixMap.put("WDC", "Western Digital");

        // 定义一个UDF用于根据model列确定品牌
        UDF1<String, String> determineBrand = (String model) -> {
            for (Map.Entry<String, String> entry : brandPrefixMap.entrySet()) {
                if (model.startsWith(entry.getKey())) {
                    return entry.getValue();
                }
            }
            return "Others";
        };

        // 注册UDF
        spark.udf().register("determineBrand", determineBrand, DataTypes.StringType);

        // 读取CSV文件并创建DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true") // 第一行作为表头
                .option("inferSchema", "true") // 自动推断数据类型
                .csv("/Users/masonwang/Downloads/data_Q3_2019/*.csv");

        // 选择需要的列并过滤不合法的行
        Dataset<Row> filteredDF = df.select("model", "failure")
                .filter("model is not null and failure is not null");

        // 添加brand列
        Dataset<Row> brandDF = filteredDF.withColumn("brand", functions.callUDF("determineBrand", filteredDF.col("model")));

        // 按品牌分组并计算failure的总和
        Dataset<Row> resultDF = brandDF.groupBy("brand")
                .agg(functions.sum("failure").alias("total_failure"));

        // 将结果写入单个CSV文件
        resultDF.coalesce(1)  // 合并为一个分区，写入单个文件
                .write()
                .option("header", "true") // 输出文件包含表头
                .csv("output/brand_failure_count");

        System.out.println("Data saved successfully to output/brand_failure_count");

        spark.stop();
    }
}
