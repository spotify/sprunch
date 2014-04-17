package com.spotify.data.examples.scala

import org.apache.crunch.{Pair => CPair, _}
import org.apache.crunch.types.avro.Avros
import org.apache.crunch.types.{PTableType, PType}
import java.lang.{Integer=>JInt, Long=>JLong, Iterable=>JIterable}
import scala._
import scala.Predef._
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import org.apache.crunch.lib.SecondarySort
import org.apache.avro.specific.SpecificRecord

object Sprunch {
  object Upgrades {
    implicit def upgrade[T](collection: PCollection[T]): SCollection[T] = new SCollection[T](collection)
    implicit def upgrade[GK, SK, V](table: PTable[GK, CPair[SK, V]]):SecondarySortableTable[GK, SK, V]
      = new SecondarySortableTable[GK, SK, V](table)
    implicit def upgrade[K, V](table: PGroupedTable[K, V]): SGroupedTable[K, V] = new SGroupedTable[K, V](table)
  }

  object Avro {
    implicit def records[T <: SpecificRecord](implicit evidence: ClassTag[T]): PType[T] = Avros.containers(evidence.runtimeClass).asInstanceOf[PType[T]]
    implicit def ints: PType[JInt] = Avros.ints
    implicit def longs: PType[JLong] = Avros.longs
    implicit def strings: PType[String] = Avros.strings
    implicit def pairs[T1, T2](implicit type1: PType[T1], type2: PType[T2]): PType[CPair[T1, T2]] = Avros.pairs(type1, type2)
    implicit def collections[T](implicit ptype: PType[T]): PType[java.util.Collection[T]] = Avros.collections(ptype)
    implicit def tableOf[K, V](implicit keyType: PType[K], valueType: PType[V]) = Avros.tableOf(keyType, valueType)
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
    def filter(acceptFn: T => Boolean) = underlying.filter(new Fns.SFilter(acceptFn))
  }

  class SecondarySortableTable[GroupKey, SortKey, Value](val underlying: PTable[GroupKey, CPair[SortKey, Value]]) {
    def secondarySort[T](fn: CPair[GroupKey, JIterable[CPair[SortKey, Value]]] => T)(implicit pType: PType[T]): PCollection[T] =
      SecondarySort.sortAndApply(underlying, new Fns.SMap(fn), pType)
  }

  class SGroupedTable[K, V](val underlying: PGroupedTable[K, V]) {
    def foldValues(initialValue: V, fn: (V, V) => V) = underlying.combineValues(new Fns.SFoldValues[K, V](initialValue, fn))
  }
}
