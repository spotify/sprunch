/*
 * Copyright 2014 Spotify AB. All rights reserved.
 *
 * The contents of this file are licensed under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.sprunch
import Sprunch.Upgrades._
import Sprunch.Avro._
import org.junit.{Assert, Test}
import org.apache.crunch.impl.mem.MemPipeline
import org.apache.crunch.types.avro.Avros
import org.apache.crunch.{Pair => CPair, PCollection, PTable}
import org.apache.crunch.lib.join.JoinType
import scala.collection.JavaConverters._
import scala.collection.mutable
import org.apache.crunch.types.PType
import scala.collection.generic.Growable

class SprunchTest {
  @Test
  def testSCollection() {
    val p: PCollection[Integer] = MemPipeline.typedCollectionOf(Avros.ints(),1,2,3)
    val strs = p.map(i => i.toString).materialize().asScala.toList
    Assert.assertEquals(Seq("1", "2", "3"), strs)
    val pairs = p.mapToTable(i => (i, i.toString)).materialize().asScala.toList
    Assert.assertEquals(Seq(CPair.of(1, "1"), CPair.of(2, "2"), CPair.of(3, "3")), pairs)
    val flatMapped = p.flatMap(i => List(i, i)).materialize().asScala.toList
    Assert.assertEquals(Seq(1, 1, 2, 2, 3, 3), flatMapped)
    val keyed = p.extractKey(_.toString).materialize().asScala.toList
    Assert.assertEquals(Seq(CPair.of("1", 1), CPair.of("2", 2), CPair.of("3", 3)), keyed)
    val filtered = p.filterBy(_ % 2 != 0).materialize().asScala.toList
    Assert.assertEquals(Seq(1, 3), filtered)
    val grouped = p.groupBy(_ % 2).materialize().asScala.toList.map(p => (p.first(), p.second().asScala.toList))
    Assert.assertEquals(Seq((0, Seq(2)), (1, Seq(1, 3))), grouped)
  }

  @Test
  def testSTable() {
    val p: PTable[Integer, String] = MemPipeline.typedTableOf(
      Avros.tableOf(Avros.ints(), Avros.strings()),
      Seq(CPair.of[Integer, String](1, "one"), CPair.of[Integer, String](2, "two")).asJava)
    val strs = p.map((i, s) => "%d -> %s".format(i, s)).materialize().asScala.toList
    Assert.assertEquals(Seq("1 -> one", "2 -> two"), strs)
  }

  @Test
  def testSGroupedTable() {
    def pair(k: String, v: Int) = CPair.of[String, Integer](k, v)
    val p: PTable[String, Integer] = MemPipeline.typedTableOf(
      Avros.tableOf(Avros.strings(), Avros.ints()),
    Seq(pair("a", 1), pair("a", 2), pair("a", 3), pair("b", 10), pair("b", 20), pair("c", 100)).asJava)
    val reduced = p.groupByKey().reduceValues(_ + _).materialize().asScala.toList
    Assert.assertEquals(Seq(pair("a", 6), pair("b", 30), pair("c", 100)), reduced)
    val folded = p.groupByKey().foldValues(100)(_ + _).materialize().asScala.toList
    Assert.assertEquals(Seq(pair("a", 106), pair("b", 130), pair("c", 200)), folded)
  }

  @Test
  def testSTableJoins() {
    def pair(k: String, v: Int) = CPair.of[String, Integer](k, v)
    val p1: PTable[String, Integer] = MemPipeline.typedTableOf(
      Avros.tableOf(Avros.strings(), Avros.ints()),
      Seq(pair("a", 1), pair("a", 2), pair("b", 10)).asJava)

    val p2: PTable[String, Integer] = MemPipeline.typedTableOf(
      Avros.tableOf(Avros.strings(), Avros.ints()),
      Seq(pair("a", 10), pair("a", 20), pair("c", 20)).asJava)

    def p2t(p: CPair[String, CPair[Integer, Integer]]) = (p.first(), p.second().first(), p.second().second())

    val j1 = p1.join(p2).materialize().asScala.toList.map(p2t)
    Assert.assertEquals(Seq(("a", 1, 10), ("a", 2, 10), ("a", 1, 20), ("a", 2, 20)), j1)

    val j2 = p1.leftJoin(p2).materialize().asScala.toList.map(p2t)
    Assert.assertEquals(Seq(("a", 1, 10), ("a", 2, 10), ("a", 1, 20), ("a", 2, 20), ("b", 10, null)), j2)

    val j3 = p1.rightJoin(p2).materialize().asScala.toList.map(p2t)
    Assert.assertEquals(Seq(("a", 1, 10), ("a", 2, 10), ("a", 1, 20), ("a", 2, 20), ("c", null, 20)), j3)

    val j4 = p1.outerJoin(p2).materialize().asScala.toList.map(p2t)
    Assert.assertEquals(Seq(("a", 1, 10), ("a", 2, 10), ("a", 1, 20), ("a", 2, 20), ("b", 10, null), ("c", null, 20)), j4)
  }

  @Test
  def testScalaPrimitives() {
    val p: PCollection[Integer] = MemPipeline.typedCollectionOf(Avros.ints(),1,2,3)
    val ints = p.map(i => i.toInt).materialize().asScala.toList
    Assert.assertEquals(Seq(1,2,3), ints)
    val longs = p.map(i => i.toLong).materialize().asScala.toList
    Assert.assertEquals(Seq(1L,2L,3L), longs)
    val floats = p.map(i => i.toFloat).materialize().asScala.toList
    Assert.assertEquals(Seq(1f,2f,3f), floats)
    val doubles = p.map(i => i.toDouble).materialize().asScala.toList
    Assert.assertEquals(Seq(1d,2d,3d), doubles)
  }

  @Test
  def testScalaImmutableCollections() {
    val p: PCollection[Integer] = MemPipeline.typedCollectionOf(Avros.ints(), 1, 2)
    val lists = p.map(i => List(i, i)).materialize().asScala.toList
    Assert.assertEquals(Seq(List(1, 1), List(2, 2)), lists)
    val seqs = p.map(i => Seq(i, i)).materialize().asScala.toList
    Assert.assertEquals(Seq(Seq(1, 1), Seq(2, 2)), seqs)
    val sets = p.map(i => Set[Int](i, i + i)).materialize().asScala.toList
    Assert.assertEquals(Seq(Set(1, 2), Set(2, 4)), sets)
    val maps = p.map(i => Map[String, Int](i.toString -> i)).materialize().asScala.toList
    Assert.assertEquals(Seq(Map("1" -> 1), Map("2" -> 2)), maps)
  }

  @Test
  def testScalaMutableCollections() {
    val p: PCollection[Integer] = MemPipeline.typedCollectionOf(Avros.ints(), 1, 2)
    val arrays = p.map(i => Array(i, i)).materialize().asScala.toList.map(_.toList)
    Assert.assertEquals(Seq(List(1, 1), List(2, 2)), arrays)
    val buffers = p.map(i => mutable.Buffer(i, i)).materialize().asScala.toList
    Assert.assertEquals(Seq(mutable.Buffer(1, 1), mutable.Buffer(2, 2)), buffers)
    val sets = p.map(i => mutable.Set[Int](i, i + i)).materialize().asScala.toList
    Assert.assertEquals(Seq(mutable.Set(1, 2), mutable.Set(2, 4)), sets)
    val maps = p.map(i => mutable.Map[String, Int](i.toString -> i)).materialize().asScala.toList
    Assert.assertEquals(Seq(mutable.Map("1" -> 1), mutable.Map("2" -> 2)), maps)

    // verify that mutable collections are actually mutable in foldValues
    def headVal[K, V](p: PTable[K, V]) = p.materialize().asScala.toList.head.second()

    val zArray = Array[Integer](0, 0)
    val fArray= headVal(p.map(i => Array(i, i)).groupBy(_ => 0).foldValues(zArray)((z, a) => { z(0) += a(0); z(1) += a(1); z }))
    Assert.assertSame(zArray, fArray)
    Assert.assertEquals(Seq(3, 3), fArray.toList)

    val zBuffer = mutable.Buffer[Integer]()
    val fBuffer = headVal(p.map(i => mutable.Buffer(i, i)).groupBy(_ => 0).foldValues(zBuffer)(_ ++= _))
    Assert.assertSame(zBuffer, fBuffer)
    Assert.assertEquals(mutable.Buffer(1, 1, 2, 2), fBuffer)

    val zSet = mutable.Set[Int]()
    val fSet = headVal(p.map(i => mutable.Set[Int](i, i + i)).groupBy(_ => 0).foldValues(zSet)(_ ++= _))
    Assert.assertSame(zSet, fSet)
    Assert.assertEquals(mutable.Set(1, 2, 4), fSet)

    val zMap = mutable.Map[String, Int]()
    val fMap = headVal(p.map(i => mutable.Map[String, Int](i.toString -> i)).groupBy(_ => 0).foldValues(zMap)(_ ++= _))
    Assert.assertSame(zMap, fMap)
    Assert.assertEquals(mutable.Map("1" -> 1, "2" -> 2), fMap)
  }

  @Test
  def testScalaTuples() {
    val p: PCollection[Integer] = MemPipeline.typedCollectionOf(Avros.ints(), 1, 2)
    val pairs = p.map(i => (i.toString, i)).materialize().asScala.toList
    Assert.assertEquals(Seq(("1", 1), ("2", 2)), pairs)
    val triples = p.map(i => (i.toString, i, i.toLong)).materialize().asScala.toList
    Assert.assertEquals(Seq(("1", 1, 1L), ("2", 2, 2L)), triples)
    val quads = p.map(i => (i.toString, i, i.toLong, i.toFloat)).materialize().asScala.toList
    Assert.assertEquals(Seq(("1", 1, 1L, 1f), ("2", 2, 2L, 2f)), quads)
    val tuple5s = p.map(i => (i.toString, i, i.toLong, i.toFloat, i.toDouble)).materialize().asScala.toList
    Assert.assertEquals(Seq(("1", 1, 1L, 1f, 1d), ("2", 2, 2L, 2f, 2d)), tuple5s)
  }
}
