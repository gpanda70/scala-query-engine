package org.gpanda70.squery.datasource

import org.gpanda70.squery.datatypes.{RecordBatch, Schema}

class InMemoryDataSource(val imSchema: Schema, val data: List[RecordBatch]) extends DataSource {
  override def schema(): Schema = imSchema

  override def scan(projection: List[String]): Seq[RecordBatch] = {
    val projectionIndices = projection.map{
      name => imSchema.fields.indexWhere((_.name == name))
    }
    data.map{ batch =>
      new RecordBatch(imSchema, projectionIndices.map(batch.field))
    }
  }
}
