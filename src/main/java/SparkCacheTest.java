import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StorageStatus;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.rdd.EsPartition;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

public class SparkCacheTest implements Serializable
{
    public static void main(String[] args)
    {
        final SparkConf esSparkConf = new SparkConf();
        final String sourceIndex = "events2/events";

        esSparkConf.setMaster("spark://192.168.43.67:7077");
        esSparkConf.setAppName("cache_test");
        esSparkConf.setJars(new String[]{"/Volumes/Data/tom/bigdata/elasticsearch-hadoop-2.4.0/dist/elasticsearch-spark_2.10-2.4.0.jar"});

        esSparkConf.set("spark.executor.memory", "4G");
        esSparkConf.set("spark.es.nodes.wan.only", "true");
        esSparkConf.set("spark.es.nodes", "localhost");
        esSparkConf.set("spark.memory.fraction", "0.9");
        esSparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        esSparkConf.set("spark.kryoserializer.buffer.max", "256m");
        esSparkConf.set("spark.rdd.compress", "true");

        final JavaSparkContext sparkContext = new JavaSparkContext(esSparkConf);
        final SQLContext sqlContext = new SQLContext(sparkContext);

        //final JavaPairRDD data = JavaEsSpark.esJsonRDD(sparkContext, sourceIndex);

        //final JavaPairRDD data = JavaEsSpark.esRDD(sparkContext, sourceIndex);

        final DataFrame data = sqlContext.read().format("org.elasticsearch.spark.sql")
                .option("es.query", "?q=name:search")
                .option("es.scroll.limit", "500000")
                .load(sourceIndex);

        data.persist(StorageLevel.MEMORY_ONLY_SER());

        System.out.println(data.count() + " items - " + data.rdd().getNumPartitions() + " partitions");

        data.first();

        data.save("./data");

        for (StorageStatus status : sparkContext.sc().getExecutorStorageStatus())
        {
            System.out.println("Memused: " + convertMB(status.memUsed()));
            System.out.println("RDD blocks: " + status.numRddBlocks());
            System.out.println("Maxmem: " + convertMB(status.maxMem()));
            System.out.println("OffHeapUsed: " + convertMB(status.offHeapUsed()));
        }
    }

    static public String convertMB(long value)
    {
        return value / (1024*1024) + " Mb";
    }
}
