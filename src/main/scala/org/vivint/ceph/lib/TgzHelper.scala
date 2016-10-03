package org.vivint.ceph.lib

import java.io.ByteArrayOutputStream
import org.kamranzafar.jtar.{ TarEntry, TarHeader, TarOutputStream }
import java.util.zip.GZIPOutputStream

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
}
