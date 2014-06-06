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

import scala._
import scala.Predef._
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import java.lang.{Integer=>JInt, Long=>JLong, Float=>JFloat, Double=>JDouble, Iterable=>JIterable, Boolean=>JBool}
import java.util.{Map=>JMap}
import org.apache.avro.specific.SpecificRecord
import org.apache.crunch.{Pair => CPair, Tuple3 => CTuple3, Tuple4 => CTuple4, TupleN => CTupleN, _}
import org.apache.crunch.types.avro.{AvroType, Avros}
import org.apache.crunch.types.{PTableType, PType}
import org.apache.avro.Schema
import org.apache.crunch.types.DeepCopier.NoOpDeepCopier

object Sprunch {

  /** Sprunch extensions for an underlying PCollection */
  class SCollection[T](val underlying: PCollection[T]) {
    /** 1-1 transformation between values */
    def map[U](mapFn: T => U)(implicit pType: PType[U]) = underlying.parallelDo(new Fns.SMap(mapFn), pType)

    /** 1-1 transformation to (key,value) pair resulting in a PTable */
    def mapToTable[K, V](mapFn: T => (K, V))(implicit pTableType: PTableType[K, V]) =
      underlying.parallelDo(new Fns.STableMap(mapFn), pTableType)

    /** 1->[0...] transformation. The output will be traversed and records emitted for each item */
    def flatMap[U](mapFn: T => TraversableOnce[U])(implicit pType: PType[U]) =
      underlying.parallelDo(new Fns.SFlatMap(mapFn), pType)

    /** Transform PCollection to PTable by extracting a key */
    def extractKey[K](extractKeyFn: T => K)(implicit pType: PType[K]) =
      underlying.by(new Fns.SMap(extractKeyFn), pType)

    /** Filter rows in a PCollection by accepting only those satisfying the given predicate */
    def filterBy(acceptFn: T => Boolean) = underlying.filter(new Fns.SFilter(acceptFn))

    /** Aliases */
    def groupBy[K](fn: T => K)(implicit pType: PType[K]) = extractKey(fn).groupByKey()
  }

  /** Sprunch extensions for an underlying PCollection */
  class STable[K, V](val underlying: PCollection[CPair[K, V]]) {
    /** Allow expressing map on PTable as a binary function instead of a function of CPairs */
    def map[U](fn: (K, V) => U)(implicit pType: PType[U]) = underlying.parallelDo(new Fns.SPairMap(fn), pType)
  }

  /** Sprunch extensions for an underlying PGroupedTable */
  class SGroupedTable[K, V](val underlying: PGroupedTable[K, V]) {

    /** Perform a foldLeft over the values in the grouped table, implemented efficiently as a combine/reduce combination */
    def foldValues(initialValue: V, fn: (V, V) => V) = underlying.combineValues(new Fns.SFoldValues[K, V](initialValue, fn))
  }

  /**
   * Implicit methods to wrap PCollection, PTable and PGroupedTable instances to add extra methods under certain conditions
   */
  object Upgrades {
    implicit def upgrade[T](collection: PCollection[T]): SCollection[T] = new SCollection[T](collection)
    implicit def upgrade[K, V](table: PCollection[CPair[K, V]]): STable[K, V] = new STable[K, V](table)
    implicit def upgrade[K, V](table: PGroupedTable[K, V]): SGroupedTable[K, V] = new SGroupedTable[K, V](table)

  }

  /**
   * Implicit PTypes for Avro serialization. Importing Avro._ will make Sprunch methods resolve their PTypes at compile
   * time based on the destination type of provided function.
   */
  object Avro {

    /* This works the same way as in AvroTypeFamily for Java boxed primitives. */
    private def avroType[T](schemaType: Schema.Type)(implicit evidence: ClassTag[T]) =
      new AvroType[T](evidence.runtimeClass.asInstanceOf[Class[T]], Schema.create(schemaType), new NoOpDeepCopier[T]())

    implicit def records[T <: SpecificRecord](implicit evidence: ClassTag[T]): PType[T] =
      Avros.containers(evidence.runtimeClass).asInstanceOf[PType[T]]

    implicit def javaInts: PType[JInt] = Avros.ints
    implicit def scalaInts: PType[Int] = avroType[Int](Schema.Type.INT)
    implicit def javaLongs: PType[JLong] = Avros.longs
    implicit def scalaLongs: PType[Long] = avroType[Long](Schema.Type.LONG)
    implicit def javaDoubles: PType[JDouble] = Avros.doubles
    implicit def scalaDoubles: PType[Double] = avroType[Double](Schema.Type.DOUBLE)
    implicit def javaFloat: PType[JFloat] = Avros.floats
    implicit def scalaFloat: PType[Float] = avroType[Float](Schema.Type.FLOAT)
    implicit def javaBool: PType[JBool] = Avros.booleans()
    implicit def scalaBool: PType[Boolean] = avroType[Boolean](Schema.Type.BOOLEAN)

    implicit def strings: PType[String] = Avros.strings

    // The following compound types will be recursively resolved into the correct PTypes at compile time
    implicit def pairs[T1, T2](implicit pType1: PType[T1], pType2: PType[T2]): PType[CPair[T1, T2]] = Avros.pairs(pType1, pType2)
    implicit def tuple3s[T1, T2, T3](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3]) = Avros.triples(pType1, pType2, pType3)
    implicit def maps[V](implicit pType: PType[V]): PType[JMap[String, V]] = Avros.maps(pType)
    implicit def collections[T](implicit pType: PType[T]): PType[java.util.Collection[T]] = Avros.collections(pType)
    implicit def tableOf[K, V](implicit keyType: PType[K], valueType: PType[V]): PTableType[K, V] = Avros.tableOf(keyType, valueType)

    implicit def scalaMap[V](implicit pType: PType[V]): PType[Map[String, V]] =
      Avros.derived(
        classOf[Map[String, V]],
        new Fns.SMap[JMap[String, V], Map[String, V]](_.asScala.toMap), // NOTE: This will copy the map, ie. _expensive_
        new Fns.SMap[Map[String, V], JMap[String, V]](_.asJava),
        maps(pType))

    // Scala tuples
    implicit def scalaPairs[T1, T2](implicit pType1: PType[T1], pType2: PType[T2]): PType[Pair[T1, T2]] =
      Avros.derived(
        classOf[Pair[T1, T2]],
        new Fns.SMap[CPair[T1, T2], Pair[T1, T2]](TypeConversions.toSPair),
        new Fns.SMap[Pair[T1, T2], CPair[T1, T2]](TypeConversions.toCPair),
        pairs(pType1, pType2))

    implicit def scalaTuple3[T1, T2, T3](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3]): PType[(T1, T2, T3)] =
      Avros.derived(
        classOf[(T1, T2, T3)],
        new Fns.SMap[CTuple3[T1, T2, T3], (T1, T2, T3)](c => (c.first(), c.second(), c.third())),
        new Fns.SMap[(T1, T2, T3), CTuple3[T1, T2, T3]](s => CTuple3.of(s._1, s._2, s._3)),
        Avros.triples(pType1, pType2, pType3))

    implicit def scalaTuple4[T1, T2, T3, T4](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4]): PType[(T1, T2, T3, T4)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4)],
        new Fns.SMap[CTuple4[T1, T2, T3, T4], (T1, T2, T3, T4)](c => (c.first(), c.second(), c.third(), c.fourth())),
        new Fns.SMap[(T1, T2, T3, T4), CTuple4[T1, T2, T3, T4]](s => CTuple4.of(s._1, s._2, s._3, s._4)),
        Avros.quads(pType1, pType2, pType3, pType4))

    implicit def scalaTuple5[T1, T2, T3, T4, T5](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5]): PType[(T1, T2, T3, T4, T5)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5])),
        new Fns.SMap[(T1, T2, T3, T4, T5), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5))

    implicit def scalaTuple6[T1, T2, T3, T4, T5, T6](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6]): PType[(T1, T2, T3, T4, T5, T6)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6))

    implicit def scalaTuple7[T1, T2, T3, T4, T5, T6, T7](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7]): PType[(T1, T2, T3, T4, T5, T6, T7)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7))

    implicit def scalaTuple8[T1, T2, T3, T4, T5, T6, T7, T8](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8]): PType[(T1, T2, T3, T4, T5, T6, T7, T8)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8))

    implicit def scalaTuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9))

    implicit def scalaTuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10))

    implicit def scalaTuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11))

    implicit def scalaTuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12))

    implicit def scalaTuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13))

    implicit def scalaTuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14))

    implicit def scalaTuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15))

    implicit def scalaTuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16))

    implicit def scalaTuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16], pType17: PType[T17]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16], c.get(16).asInstanceOf[T17])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef], s._17.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16, pType17))

    implicit def scalaTuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16], pType17: PType[T17], pType18: PType[T18]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16], c.get(16).asInstanceOf[T17], c.get(17).asInstanceOf[T18])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef], s._17.asInstanceOf[AnyRef], s._18.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16, pType17, pType18))

    implicit def scalaTuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16], pType17: PType[T17], pType18: PType[T18], pType19: PType[T19]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16], c.get(16).asInstanceOf[T17], c.get(17).asInstanceOf[T18], c.get(18).asInstanceOf[T19])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef], s._17.asInstanceOf[AnyRef], s._18.asInstanceOf[AnyRef], s._19.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16, pType17, pType18, pType19))

    implicit def scalaTuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16], pType17: PType[T17], pType18: PType[T18], pType19: PType[T19], pType20: PType[T20]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16], c.get(16).asInstanceOf[T17], c.get(17).asInstanceOf[T18], c.get(18).asInstanceOf[T19], c.get(19).asInstanceOf[T20])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef], s._17.asInstanceOf[AnyRef], s._18.asInstanceOf[AnyRef], s._19.asInstanceOf[AnyRef], s._20.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16, pType17, pType18, pType19, pType20))

    implicit def scalaTuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16], pType17: PType[T17], pType18: PType[T18], pType19: PType[T19], pType20: PType[T20], pType21: PType[T21]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16], c.get(16).asInstanceOf[T17], c.get(17).asInstanceOf[T18], c.get(18).asInstanceOf[T19], c.get(19).asInstanceOf[T20], c.get(20).asInstanceOf[T21])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef], s._17.asInstanceOf[AnyRef], s._18.asInstanceOf[AnyRef], s._19.asInstanceOf[AnyRef], s._20.asInstanceOf[AnyRef], s._21.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16, pType17, pType18, pType19, pType20, pType21))

    implicit def scalaTuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3], pType4: PType[T4], pType5: PType[T5], pType6: PType[T6], pType7: PType[T7], pType8: PType[T8], pType9: PType[T9], pType10: PType[T10], pType11: PType[T11], pType12: PType[T12], pType13: PType[T13], pType14: PType[T14], pType15: PType[T15], pType16: PType[T16], pType17: PType[T17], pType18: PType[T18], pType19: PType[T19], pType20: PType[T20], pType21: PType[T21], pType22: PType[T22]): PType[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22)] =
      Avros.derived(
        classOf[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22)],
        new Fns.SMap[CTupleN, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22)](c => (c.get(0).asInstanceOf[T1], c.get(1).asInstanceOf[T2], c.get(2).asInstanceOf[T3], c.get(3).asInstanceOf[T4], c.get(4).asInstanceOf[T5], c.get(5).asInstanceOf[T6], c.get(6).asInstanceOf[T7], c.get(7).asInstanceOf[T8], c.get(8).asInstanceOf[T9], c.get(9).asInstanceOf[T10], c.get(10).asInstanceOf[T11], c.get(11).asInstanceOf[T12], c.get(12).asInstanceOf[T13], c.get(13).asInstanceOf[T14], c.get(14).asInstanceOf[T15], c.get(15).asInstanceOf[T16], c.get(16).asInstanceOf[T17], c.get(17).asInstanceOf[T18], c.get(18).asInstanceOf[T19], c.get(19).asInstanceOf[T20], c.get(20).asInstanceOf[T21], c.get(21).asInstanceOf[T22])),
        new Fns.SMap[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21, T22), CTupleN](s => CTupleN.of(s._1.asInstanceOf[AnyRef], s._2.asInstanceOf[AnyRef], s._3.asInstanceOf[AnyRef], s._4.asInstanceOf[AnyRef], s._5.asInstanceOf[AnyRef], s._6.asInstanceOf[AnyRef], s._7.asInstanceOf[AnyRef], s._8.asInstanceOf[AnyRef], s._9.asInstanceOf[AnyRef], s._10.asInstanceOf[AnyRef], s._11.asInstanceOf[AnyRef], s._12.asInstanceOf[AnyRef], s._13.asInstanceOf[AnyRef], s._14.asInstanceOf[AnyRef], s._15.asInstanceOf[AnyRef], s._16.asInstanceOf[AnyRef], s._17.asInstanceOf[AnyRef], s._18.asInstanceOf[AnyRef], s._19.asInstanceOf[AnyRef], s._20.asInstanceOf[AnyRef], s._21.asInstanceOf[AnyRef], s._22.asInstanceOf[AnyRef])),
        Avros.tuples(pType1, pType2, pType3, pType4, pType5, pType6, pType7, pType8, pType9, pType10, pType11, pType12, pType13, pType14, pType15, pType16, pType17, pType18, pType19, pType20, pType21, pType22))
  }

  /**
   * Conversions between Crunch/Java and native Scala types
   */
   object TypeConversions {
    /** Convert a Scala pair to a Crunch Pair */
    def toCPair[K, V](pair: (K, V)) = CPair.of(pair._1, pair._2)

    /** Convert a Crunch pair to a Scala Pair */
    def toSPair[K, V](pair: CPair[K, V]) = (pair.first(), pair.second())
  }

  /**
   * Function wrappers to wrap lambda functions and named functions into Crunch ...Fn types.
   */
  object Fns {
    /** Wrap simple function into MapFn */
    class SMap[T, U](fn: T=>U) extends MapFn[T, U] {
      override def map(input: T) = fn(input)
    }

    /** Wrap Scala pair-parametered function into a MapFn accepting a CPair */
    class SPairMap[K, V, U](fn: (K, V) => U) extends MapFn[CPair[K, V], U] {
      override def map(input: CPair[K, V]): U = fn(input.first(), input.second())
    }

    /** Wrap Scala pair-valued function into a MapFn suitable for creating a PTable */
    class STableMap[T, K, V](fn: T => (K, V)) extends MapFn[T, CPair[K, V]] {
      override def map(input: T) = TypeConversions.toCPair(fn(input))
    }

    /** Wrap a traversable-valued function into a DoFn which emits everything in the traversable */
    class SFlatMap[T, U](fn: T=>TraversableOnce[U]) extends DoFn[T, U] {
      override def process(input: T, emitter: Emitter[U]) = fn(input).foreach(emitter.emit)
    }

    /** Wrap a boolean-valued function into a FilterFn */
    class SFilter[T](fn: T => Boolean) extends FilterFn[T] {
      override def accept(input: T) = fn(input)
    }

    /** Convert an initial value and fold function into a CombineFn which will fold left over values */
    class SFoldValues[K, V](initial: V, fn: (V, V) => V) extends CombineFn[K, V] {
      override def process(input: CPair[K, JIterable[V]], emitter: Emitter[CPair[K, V]]) =
        emitter.emit(CPair.of(input.first(), input.second().asScala.foldLeft(initial)(fn)))
    }
  }
}
