package org.gpanda70.squery.datatypes

/**
 * Batch of data in column format
 * @param schema The schema that represents the Column RecordBatch
 * @param fields The column vector that has the actual data behind RecordBatch
 */
class RecordBatch(val schema: Schema, val fields: List[ColumnVector]){

  def rowCount(): Int = fields.head.getSize()

  def columnCount(): Int = fields.size

  def field(i: Int): ColumnVector = fields(i)

  // Efficient CSV creation using StringBuilder
  def toCSV(): String = {
    val stringBuilder = new StringBuilder
    for (rowIndex <- 0 until rowCount()) {
      for (columnIndex <- 0 until columnCount()) {
        if (columnIndex > 0) stringBuilder.append(",")

        val v = fields(columnIndex)
        val value = v.getValue(rowIndex)

        value match {
          case null => stringBuilder.append("null")
          case bytes: Array[Byte] => stringBuilder.append(new String(bytes))
          case _ => stringBuilder.append(value)
        }
      }
      stringBuilder.append("\n")
    }
    stringBuilder.toString()
  }

  override def toString(): String = toCSV()

}
