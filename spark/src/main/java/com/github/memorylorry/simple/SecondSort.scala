package com.github.memorylorry.simple

class SecondSort extends Ordered[SecondSort] with Serializable {
  var first = 0;
  var second = 0;

  def this(first: Int, second:Int){
    this()
    this.first = first
    this.second = second
  }

  override def compare(that: SecondSort): Int = {
    if(this.first != that.first)
      this.first - that.first
    else
      that.second - this.second
  }
}
