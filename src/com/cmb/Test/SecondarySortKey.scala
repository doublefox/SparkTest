package com.cmb.Test

/**
  * Created by ChenNing on 2017/5/13.
  * 排序组合键，继承比较器Ordered
  */
class SecondarySortKey(val name: String, val first: Int, val second: Int) extends Ordered[SecondarySortKey] with Serializable {
  /**
    * 负数this<that 0 this=that 正数 this>that
    *
    * @param that
    * @return
    */
  override def compare(that: SecondarySortKey): Int = {

    this.name.equals(that.name) match {
      case false => {

        -this.name.compareTo(that.name)
      }
      case _ => {
        if (this.first - that.first != 0) {
          this.first - that.first
        } else {
          that.second - this.second
        }
      }
    }
  }
}