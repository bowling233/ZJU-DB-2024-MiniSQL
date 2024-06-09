#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t offset = 0;
  // rid
  memcpy(buf + offset, &rid_, sizeof(RowId));
  offset += sizeof(RowId);
  // null bitmap
  uint32_t null_size = (fields_.size() + 7) / 8;
  char null_bitmap[null_size];
  memset(null_bitmap, 0, null_size);
  for (uint32_t i = 0; i < fields_.size(); i++) {
    if (fields_[i]->IsNull()) {
      null_bitmap[i / 8] |= (1 << (i % 8));
    }
  }
  memcpy(buf + offset, null_bitmap, null_size);
  offset += null_size;
  // fields
  for (uint32_t i = 0; i < fields_.size(); i++) {
    offset += fields_[i]->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t offset = 0;
  // rid
  RowId temp=rid_;
  memcpy(&temp, buf + offset, sizeof(RowId));
  offset += sizeof(RowId);
  // null bitmap
  uint32_t null_size = (schema->GetColumnCount() + 7) / 8;
  char null_bitmap[null_size];
  memcpy(null_bitmap, buf + offset, null_size);
  offset += null_size;
  // fields
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    TypeId type = schema->GetColumn(i)->GetType();
    Field *field = new Field(type);
    offset += field->DeserializeFrom(buf + offset, type, &field, null_bitmap[i / 8] & (1 << (i % 8)));
    fields_.push_back(field);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = sizeof(RowId);
  uint32_t null_size = (fields_.size() + 7) / 8;
  size += null_size;
  for (auto field : fields_) {
    size += field->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
