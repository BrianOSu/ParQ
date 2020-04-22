/*
   Copyright 2020 Brian O'Sullivan

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <writer.hpp>

using namespace KDB::PARQ;

std::shared_ptr<parquet::ParquetFileWriter> WRITER::OpenFile(std::string fileName, 
                                                             std::shared_ptr<GroupNode> schema,
                                                             parquet::Compression::type codec,
                                                             bool append){
    auto out_file = arrow::io::FileOutputStream::Open(fileName, append);
    parquet::WriterProperties::Builder builder;
    builder.compression(codec);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    return parquet::ParquetFileWriter::Open(out_file.ValueOrDie(), schema, props);
}

std::shared_ptr<GroupNode> WRITER::SetupSchema(K &names, K &values, int numCols){
    parquet::schema::NodeVector fields;
    for(int i=0;i<numCols;i++){
        int colType = kK(values)[i]->t;
        int firstType = colType == 0? kK(kK(values)[i])[0]->t : 0;
        fields.push_back(k2parquet(kS(names)[i], colType, firstType));
    }
    return std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

parquet::schema::NodePtr WRITER::k2parquet(const std::string& name, int type, int firstType){
    if(type == KB)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::None(), Type::BOOLEAN);
    #if KXVER>=3
    else if(type == UU)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::UUID(), Type::FIXED_LEN_BYTE_ARRAY, 16);
    #endif
    else if(type == KG)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::None(), Type::FIXED_LEN_BYTE_ARRAY, 1);
    else if(type == KH)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Int(16, true), Type::INT32);
    else if(type == KI)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Int(32, true), Type::INT32);
    else if(type == KM || type == KU || type == KV)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::None(), Type::INT32);
    else if(type == KJ)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Int(64, true), Type::INT64);
    else if(type == KE)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::None(), Type::FLOAT);
    else if(type == KF || type == KZ)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::None(), Type::DOUBLE);
    else if(type == KC)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::String(), Type::BYTE_ARRAY);
    else if(type == KS || (20 <= type && type <= 76))
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Enum(), Type::BYTE_ARRAY);
    else if(type == KP)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Timestamp(false, LogicalType::TimeUnit::unit::NANOS), Type::INT64);
    else if(type == KN)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Time(false, LogicalType::TimeUnit::unit::NANOS), Type::INT64);
    else if(type == KD)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Date(), Type::INT32);
    else if(type == KT)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::Time(false, LogicalType::TimeUnit::unit::MILLIS), Type::INT32);
    else if(firstType == KC)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::String(), Type::BYTE_ARRAY);
    else
        return PrimitiveNode::Make(name, Repetition::REQUIRED, parquet::LogicalType::None(), Type::BYTE_ARRAY);
}

void WRITER::writeColumn(K &col, parquet::RowGroupWriter* rg_writer){
    int type = col->t;
    if(type == KB)
        writeCol(static_cast<parquet::BoolWriter*>(rg_writer->NextColumn()), col->n, &kB(col)[0]);
    #if KXVER>=3
    else if(type == UU)
        writeGuidCol(static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn()), col);
    #endif
    else if(type == KG)
        writeByteCol(static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn()), col);
    else if(type == KH)
        writeShortCol(static_cast<parquet::Int32Writer*>(rg_writer->NextColumn()), col);
    else if(type == KI || type == KM || type == KD || type == KU || type == KV || type == KT)
        writeCol(static_cast<parquet::Int32Writer*>(rg_writer->NextColumn()), col->n, &kI(col)[0]);
    else if(type == KJ | type == KP || type == KN)
        writeCol(static_cast<parquet::Int64Writer*>(rg_writer->NextColumn()), col->n, &kJ64(col)[0]);
    else if(type == KE)
        writeCol(static_cast<parquet::FloatWriter*>(rg_writer->NextColumn()), col->n, &kE(col)[0]);
    else if(type == KF || type == KZ)
        writeCol(static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn()), col->n, &kF(col)[0]);
    else if(type == KC)
        writeCharCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
    else if(type == KS)
        writeSymCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
    else if(20 <= type && type <= 76)
        writeSymCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), d9(b9(-1,col)));
    else
        writeCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
}

template<typename T, typename T1>
void WRITER::writeCol(T writer, int len, T1 col){
    writer->WriteBatch(len, nullptr, nullptr, col);
}

#if KXVER>=3
void WRITER::writeGuidCol(parquet::FixedLenByteArrayWriter* writer, K &col){
    writer->WriteBatch(col->n, nullptr, nullptr, &std::vector<parquet::FixedLenByteArray>(&kU(col)->g, &kU(col)->g + col->n)[0]);
}
#endif

void WRITER::writeByteCol(parquet::FixedLenByteArrayWriter* writer, K &col){
    writer->WriteBatch(col->n, nullptr, nullptr, &std::vector<parquet::FixedLenByteArray>(&kG(col), &kG(col) + col->n)[0]);
}

void WRITER::writeShortCol(parquet::Int32Writer* writer, K &col){
    writer->WriteBatch(col->n, nullptr, nullptr, &std::vector<int32_t>(&kH(col)[0], &kH(col)[0] + col->n)[0]);
}

void WRITER::writeCol(parquet::Int96Writer* writer, K &col){
    for (int i = 0; i < col->n; i++){
        parquet::Int96 value;
        //Magic number that adjusts for julian days
        value.value[2]=2451545;
        parquet::Int96SetNanoSeconds(value,kJ(col)[i]);
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}

void WRITER::writeCharCol(parquet::ByteArrayWriter* writer, K &col){
    writer->WriteBatch(col->n, nullptr, nullptr, &byteToVec(col)[0]);
}

std::vector<parquet::ByteArray> WRITER::byteToVec(K &col){
    std::vector<parquet::ByteArray> buffer;
    for(int i=0; i<col->n; i++)
        buffer.push_back(parquet::ByteArray(1,&kG(col)[i]));
    return buffer;
}

void WRITER::writeSymCol(parquet::ByteArrayWriter* writer, K col){
    writer->WriteBatch(col->n, nullptr, nullptr, &stringToVec(col)[0]);
}

std::vector<parquet::ByteArray> WRITER::stringToVec(K &col){
    std::vector<parquet::ByteArray> buffer;
    for(int i=0; i<col->n; i++)
        buffer.push_back(parquet::ByteArray(std::string(kS(col)[i]).length(),reinterpret_cast<unsigned char*>(kS(col)[i])));
    return buffer;
}

void WRITER::writeCol(parquet::ByteArrayWriter* writer, K &col){
    writer->WriteBatch(col->n, nullptr, nullptr, &kToVec(col)[0]);
}

std::vector<parquet::ByteArray> WRITER::kToVec(K &col){
    std::vector<parquet::ByteArray> buffer;
    for(int i=0; i<col->n; i++)
        buffer.push_back(parquet::ByteArray(kK(col)[i]->n,kG(kK(col)[i])));
    return buffer;
}