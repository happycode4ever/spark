object Test {
  //f(1)=1
  //f(2)*1.996=f(1)+f(2)+2
  //f(3)*1.996=f(1)+f(2)+f(3)+3

  def sum(x:Int):Double={
    if(x==1)1d
    else{
      var cash:Double=0d
      for(i <- 1 to x-1){
        cash += (sum(i) + 1)
      }
      cash
    }
  }

  def main(args: Array[String]): Unit = {
    var s = 0d
    (1 to 10).foreach(x=>{
      val value = sum(x)
      s+=value
      println(s"$x: $value sum=$s")})
  }
}
