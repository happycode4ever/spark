package com.jj.spark.streaming.input.custom

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import com.google.common.base.Charsets
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

//自定义采集数据源，需要继承抽象类Receiver[T]T表示接收的数据类型，还要指定存储格式
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  override def onStart(): Unit = {
    //通过socket读取
    val socket = new Socket(host, port)
    //通过转换包装流读取一行
    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream, Charsets.UTF_8))
    var line: String = null
    //循环读取一样
    while (!isStopped && (line = reader.readLine()) != null) {
      //如果接收到数据就保存
      store(line)
    }
    //释放资源
    reader.close()
    socket.close()

    //如果while停止了重新执行
    restart("CustomReceiver restart")

  }

  override def onStop(): Unit = {}
}
