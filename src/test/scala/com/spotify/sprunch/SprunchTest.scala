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
import org.apache.crunch.PCollection
import scala.collection.JavaConverters._

class SprunchTest {
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
