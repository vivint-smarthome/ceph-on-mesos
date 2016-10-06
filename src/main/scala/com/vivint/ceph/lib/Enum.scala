package com.vivint.ceph.lib

trait Enum { //DIY enum type
  type EnumVal <: Value //This is a type that needs to be found in the implementing class

  //This is the trait that we need to extend our EnumVal type with, it does the book-keeping for us
  protected trait Value { self: EnumVal => //Enforce that no one mixes in Value in a non-EnumVal type
    def name: String //All enum values should have a name

    override def toString = name //And that name is used for the toString operation
    override def equals(other: Any) = this eq other.asInstanceOf[AnyRef]
    override def hashCode = 31 * (this.getClass.## + name.##)
  }

  def values: Vector[EnumVal]
}
