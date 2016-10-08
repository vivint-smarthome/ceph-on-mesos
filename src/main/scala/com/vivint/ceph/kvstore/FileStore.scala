package com.vivint.ceph.kvstore

import akka.Done
import akka.stream.scaladsl.Keep
import java.io.{ File, FileInputStream, FileOutputStream, RandomAccessFile }
import java.util.Arrays
import java.util.concurrent.Executors
import org.apache.commons.io.IOUtils
import scala.collection.immutable.Seq
import scala.concurrent.{ ExecutionContext, Future, Promise }
import scala.concurrent.duration._
import akka.stream.scaladsl.Source

/** For use in development. Connecting to zookeeper with curator takes time.
  */
class FileStore(basePath: File) extends KVStore {

  implicit private val ec = ExecutionContext.fromExecutor(
    Executors.newSingleThreadExecutor())

  private def fileFor(path: String) =
    new File(basePath, path)
  def create(path: String, data: Array[Byte]): Future[Unit] = Future {
    val output = fileFor(path)
    if (output.exists()) {
      throw new RuntimeException(s"path ${path} already exists")
    }

    output.getParentFile.mkdirs()
    val f = new FileOutputStream(output)
    IOUtils.writeChunked(data, f)
    f.close()
  }

  def set(path: String, data: Array[Byte]): Future[Unit] = Future {
    val output = fileFor(path)
    val f = new FileOutputStream(output)
    IOUtils.writeChunked(data, f)
    f.close()
  }

  private [this] def createAndSetSync(output: File, data: Array[Byte]): Unit = {
    output.getParentFile.mkdirs()
    val f = new FileOutputStream(output)
    IOUtils.writeChunked(data, f)
    f.close()
  }

  def createAndSet(path: String, data: Array[Byte]): Future[Unit] = Future {
    val output = fileFor(path)
    createAndSetSync(output, data)
  }

  def delete(path: String): Future[Unit] = Future {
    val output = fileFor(path)
    if (output.exists())
      output.delete()
  }

  def get(path: String): Future[Option[Array[Byte]]] = Future {
    val input = fileFor(path)
    if (input.exists()) {
      val f = new FileInputStream(input)
      val data = IOUtils.toByteArray(f)
      f.close()
      Some(data)
    } else {
      None
    }
  }

  def lock(path: String): Future[KVStore.CancellableWithResult] = Future {
    val lockFile = fileFor(path)
    createAndSetSync(lockFile, Array.empty)
    val raf = new RandomAccessFile(lockFile, "rw")
    val lock = raf.getChannel().tryLock()
    if (lock == null)
      throw new RuntimeException(s"Could not acquire lock for ${path}")

    val p = Promise[Done]
    p.future.onComplete(_ => lock.release())

    new KVStore.CancellableWithResult {
      def result = p.future
      def cancel(): Boolean = { p.trySuccess(Done) }
      def isCancelled = p.future.isCompleted
    }
  }

  def children(path: String): Future[Seq[String]] = Future {
    Option(fileFor(path).listFiles).
      map(_.toList).
      getOrElse(Nil).
      map { f =>
        f.getName
      }
  }

  def watch(path: String, bufferSize: Int = 1): Source[Option[Array[Byte]], KVStore.CancellableWithResult] = {
    val first = Source.fromIterator( () => Iterator(path) ).mapAsync(1)(get)

    val updates = Source.tick(0.seconds, 1.second, path).mapMaterializedValue { cancellable =>
      val cancelled = Promise[Done]

      new KVStore.CancellableWithResult {
        def result = cancelled.future
        def cancel(): Boolean = {
          cancelled.trySuccess(Done)
          cancellable.cancel
        }
        def isCancelled = {
          cancellable.isCancelled
        }
      }
    }.mapAsync(1)(get).
      sliding(2,1).
      mapConcat {
        case Vector(None, e @ Some(_)) =>
          List(e)
        case Vector(Some(_), None) =>
          List(None)
        case Vector(Some(a), e @ Some(b)) if ! Arrays.equals(a, b) =>
          List(e)
        case Vector(e) =>
          List(e)
        case _ =>
          Nil
      }

    first.concatMat(updates)(Keep.right)
  }
}
