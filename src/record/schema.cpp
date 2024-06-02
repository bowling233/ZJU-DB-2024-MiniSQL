#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  // magic number
  memcpy(buf + offset, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  // column count
  uint32_t column_count = static_cast<uint32_t>(columns_.size());
  memcpy(buf + offset, &column_count, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  // columns
  for (const auto &column : columns_) {
    offset += column->SerializeTo(buf + offset);
  }
  // is_manage
  memcpy(buf + offset, &is_manage_, sizeof(bool));
  offset += sizeof(bool);
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t);  // magic number
  size += sizeof(uint32_t);  // column count
  for (const auto &column : columns_) {
    size += column->GetSerializedSize();
  }
  size += sizeof(bool);  // is_manage
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  // magic number
  uint32_t magic_num;
  memcpy(&magic_num, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid schema magic number.");
  // column count
  uint32_t column_count;
  memcpy(&column_count, buf + offset, sizeof(uint32_t));
  offset += sizeof(uint32_t);
  // columns
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_count; i++) {
    Column *column;
    offset += Column::DeserializeFrom(buf + offset, column);
    columns.push_back(column);
  }
  // is_manage
  bool is_manage;
  memcpy(&is_manage, buf + offset, sizeof(bool));
  offset += sizeof(bool);
  schema = new Schema(columns, is_manage);
  return offset;
}