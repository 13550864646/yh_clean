package action

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {
  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setAppName("wordCount").setMaster("local[2]")
    val ss=new StreamingContext(sparkConf,Seconds(3)) //每15秒监听一次sreaming文件夹
    val lines=ss.textFileStream("hdfs://172.18.55.28:8020/cusReport/inputdata/JA/gzmd")
    val words=lines.flatMap(_.split(" "))
    val wordCounts=words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    wordCounts.print()
    ss.start()
    ss.awaitTermination()
  }
}
