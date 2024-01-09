package org.gpanda70.squery.datasource

import org.gpanda70.squery.datatypes.{RecordBatch, Schema}
trait DataSource {
  def schema(): Schema

  def scan(projection: List[String]): Seq[RecordBatch]
}
