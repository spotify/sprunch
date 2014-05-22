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
import org.apache.crunch.{Pair => CPair, _}
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
