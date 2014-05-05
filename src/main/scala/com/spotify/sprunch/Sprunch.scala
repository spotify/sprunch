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
import org.apache.crunch.lib.SecondarySort
import org.apache.avro.Schema
import org.apache.crunch.types.DeepCopier.NoOpDeepCopier
import java.util

object Sprunch {
  object Upgrades {
    implicit def upgrade[T](collection: PCollection[T]): SCollection[T] = new SCollection[T](collection)
    implicit def upgrade[GK, SK, V](table: PTable[GK, CPair[SK, V]]):SecondarySortableTable[GK, SK, V]
      = new SecondarySortableTable[GK, SK, V](table)
    implicit def upgrade[K, V](table: PGroupedTable[K, V]): SGroupedTable[K, V] = new SGroupedTable[K, V](table)
  }

  object Avro {
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
    implicit def pairs[T1, T2](implicit pType1: PType[T1], pType2: PType[T2]): PType[CPair[T1, T2]] = Avros.pairs(pType1, pType2)
    implicit def tuple3s[T1, T2, T3](implicit pType1: PType[T1], pType2: PType[T2], pType3: PType[T3]) = Avros.triples(pType1, pType2, pType3)
    implicit def maps[V](implicit pType: PType[V]): PType[JMap[String, V]] = Avros.maps(pType)
    implicit def collections[T](implicit pType: PType[T]): PType[java.util.Collection[T]] = Avros.collections(pType)
    implicit def tableOf[K, V](implicit keyType: PType[K], valueType: PType[V]): PTableType[K, V] = Avros.tableOf(keyType, valueType)
  }

  object Fns {
    class SMap[T, U](fn: T=>U) extends MapFn[T, U] {
      override def map(input: T) = fn(input)
    }
    class STableMap[T, K, V](fn: T => (K, V)) extends MapFn[T, CPair[K, V]] {
      override def map(input: T) = toCPair(fn(input))
    }
    class SFlatMap[T, U](fn: T=>TraversableOnce[U]) extends DoFn[T, U] {
      override def process(input: T, emitter: Emitter[U]) = fn(input).foreach(emitter.emit)
    }
    class SFilter[T](fn: T => Boolean) extends FilterFn[T] {
      override def accept(input: T) = fn(input)
    }

    class SFoldValues[K, V](initial: V, fn: (V, V) => V) extends CombineFn[K, V] {
      override def process(input: CPair[K, JIterable[V]], emitter: Emitter[CPair[K, V]]) =
        emitter.emit(CPair.of(input.first(), input.second().asScala.foldLeft(initial)(fn)))
    }

    private def toCPair[K, V](pair: (K, V)) = CPair.of(pair._1, pair._2)
  }

  class SCollection[T](val underlying: PCollection[T]) {
    def map[U](mapFn: T => U)(implicit pType: PType[U]) = underlying.parallelDo(new Fns.SMap(mapFn), pType)
    def mapToTable[K, V](mapFn: T => (K, V))(implicit pTableType: PTableType[K, V]) =
      underlying.parallelDo(new Fns.STableMap(mapFn), pTableType)
    def flatMap[U](mapFn: T => TraversableOnce[U])(implicit pType: PType[U]) =
      underlying.parallelDo(new Fns.SFlatMap(mapFn), pType)
    def extractKey[K](extractKeyFn: T => K)(implicit pType: PType[K]) =
      underlying.by(new Fns.SMap(extractKeyFn), pType)
    def filterBy(acceptFn: T => Boolean) = underlying.filter(new Fns.SFilter(acceptFn))
  }

  class SecondarySortableTable[GroupKey, SortKey, Value](val underlying: PTable[GroupKey, CPair[SortKey, Value]]) {
    def secondarySort[T](fn: CPair[GroupKey, JIterable[CPair[SortKey, Value]]] => T)(implicit pType: PType[T]): PCollection[T] =
      SecondarySort.sortAndApply(underlying, new Fns.SMap(fn), pType)
  }

  class SGroupedTable[K, V](val underlying: PGroupedTable[K, V]) {
    def foldValues(initialValue: V, fn: (V, V) => V) = underlying.combineValues(new Fns.SFoldValues[K, V](initialValue, fn))
  }
}
