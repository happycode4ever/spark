package com.jj.rdd.transform

object HashTest {
  def main(args: Array[String]): Unit = {
    def nonNegativeMod(x: Int, mod: Int): Int = {
      val rawMod = x % mod
      rawMod + (if (rawMod < 0) mod else 0)
    }
    val hash = -100
    println(hash)
    println(nonNegativeMod(hash, 3))
  }
}
