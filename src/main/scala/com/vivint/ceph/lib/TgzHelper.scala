package com.vivint.ceph.lib

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream }
import java.util.zip.GZIPInputStream
import org.apache.commons.io.IOUtils
import org.kamranzafar.jtar.{ TarEntry, TarHeader, TarInputStream, TarOutputStream }
import java.util.zip.GZIPOutputStream
import scala.collection.{Iterator,breakOut}

object TgzHelper {
  def octal(digits: String): Int =
    BigInt(digits, 8).toInt

  case class FileEntry(mode: Int, data: Array[Byte])

  def makeTgz(files: (String, FileEntry)*) = {
    val dest = new ByteArrayOutputStream
    val tgz = new TarOutputStream(new GZIPOutputStream(dest))
    val now = System.currentTimeMillis / 1000

    files.foreach { case (file, entry) =>
      tgz.putNextEntry(new TarEntry(
        TarHeader.createHeader(
          file,
          entry.data.length.toLong,
          now, false, entry.mode)))
      tgz.write(entry.data, 0, entry.data.length)
    }
    tgz.close()
    dest.toByteArray()
  }

  class TarIterator(s: TarInputStream) extends Iterator[(TarEntry, Array[Byte])] {
    var _nextEntry: (TarEntry, Array[Byte]) = null
    private def loadNext(): Unit =
      _nextEntry = s.getNextEntry match {
        case null => null
        case entry => (entry, IOUtils.toByteArray(s))
      }

    def hasNext = _nextEntry != null
    def next() = {
      val nextResult = _nextEntry
      loadNext()
      nextResult
    }
    loadNext()
  }

  def readTgz(tgz: Array[Byte]): Iterator[(String, Array[Byte])] = {
    val input = new ByteArrayInputStream(tgz)
    val stream = new TarInputStream(new GZIPInputStream(input))

    new TarIterator(stream).map {
      case (entry, data) => entry.getName -> data
    }
  }

}
