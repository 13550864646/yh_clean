package action

import com.alibaba.fastjson.JSON
import entity.JA.dt.{dt_desc, dt_res_data}
import entity.TZ.tz_res_data
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization.read

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "d")
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ha")
    sc.hadoopConfiguration.set("dfs.nameservices", "ha")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ha", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn1", "yunhe.bigdata-master.yhwx:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "yunhe.bigdata-slave.yhwx:8020")
    //    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "192.168.1.235:33003")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ha", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    val ssc = new StreamingContext(sc, Seconds(3))
    val sql = SparkSession.builder().config(conf).getOrCreate()
    import sql.implicits._


//    val res_data = sql.sql("select j.realName, j.idCard, j.regfirsttime," +
//      "t.loan_offer_sum, t.verif_count, t.ave_repay_amount_level, " +
//      "b.tot_mons_str, b.oth_orgnum_str, b.m12_nsloan_orgnum_str, b.m12_night_allnum_str " +
//      "from ja j " +
//      "join tz t on j.idCard = t.idCard " +
//      "join br b on j.idCard = b.idCard")
//    res_data.toDF().write.mode(SaveMode.Append).csv("hdfs://ha/sparkStreaminput/mydata")

    sql.read.csv("hdfs://ha/sparkStreaminput/mydata").repartition(1).toDF().write.mode(SaveMode.Append).csv("file:///F:/mydata")

  }
}
