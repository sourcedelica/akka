/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.dispatch.sysmsg

import akka.testkit.AkkaSpec

class SystemMessageListSpec extends AkkaSpec {
  import SystemMessageList.Nil

  "The SystemMessageList value class" must {

    "handle empty lists correctly" in {
      val o = Array(Create(0) :: Nil)
      println(o)
      Nil.head must be === null
      Nil.isEmpty must be(true)
      (Nil.reverse == Nil) must be(true)
    }

    "able to append messages" in {
      val create0 = Create(0)
      val create1 = Create(1)
      val create2 = Create(2)
      ((create0 :: Nil).head eq create0) must be(true)
      ((create1 :: create0 :: Nil).head eq create1) must be(true)
      ((create2 :: create1 :: create0 :: Nil).head eq create2) must be(true)

      (create2.next eq create1) must be(true)
      (create1.next eq create0) must be(true)
      (create0.next eq null) must be(true)
    }

    "able to deconstruct head and tail" in {
      val create0 = Create(0)
      val create1 = Create(1)
      val create2 = Create(2)
      val list = create2 :: create1 :: create0 :: Nil

      (list.head eq create2) must be(true)
      (list.tail.head eq create1) must be(true)
      (list.tail.tail.head eq create0) must be(true)
      (list.tail.tail.tail.head eq null) must be(true)
    }

    "properly report size and emptyness" in {
      val create0 = Create(0)
      val create1 = Create(1)
      val create2 = Create(2)
      val list = create2 :: create1 :: create0 :: Nil

      list.size must be === 3
      list.isEmpty must be(false)

      list.tail.size must be === 2
      list.tail.isEmpty must be(false)

      list.tail.tail.size must be === 1
      list.tail.tail.isEmpty must be(false)

      list.tail.tail.tail.size must be === 0
      list.tail.tail.tail.isEmpty must be(true)

    }

    "properly reverse contents" in {
      val create0 = Create(0)
      val create1 = Create(1)
      val create2 = Create(2)
      val list = create2 :: create1 :: create0 :: Nil
      val listRev = list.reverse

      listRev.isEmpty must be(false)
      listRev.size must be === 3

      (listRev.head eq create0) must be(true)
      (listRev.tail.head eq create1) must be(true)
      (listRev.tail.tail.head eq create2) must be(true)
      (listRev.tail.tail.tail.head eq null) must be(true)

      (create0.next eq create1) must be(true)
      (create1.next eq create2) must be(true)
      (create2.next eq null) must be(true)
    }

  }

}
