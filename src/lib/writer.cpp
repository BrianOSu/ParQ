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
    for(int i=0;i<numCols;i++)
        fields.push_back(k2parquet(kS(names)[i], kK(values)[i]->t));
    return std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

parquet::schema::NodePtr WRITER::k2parquet(const std::string& name,int type){
    if(type == KB)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::BOOLEAN, parquet::ConvertedType::NONE);
    else if(type == UU)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE, 16);
    else if(type == KG)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE, 1);
    else if(type == KH)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::INT_16);
    else if(type == KI || type == KM || type == KU || type == KV)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::NONE);
    else if(type == KJ)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64, parquet::ConvertedType::INT_64);
    else if(type == KE)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FLOAT, parquet::ConvertedType::NONE);
    else if(type == KF || type == KZ)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::DOUBLE, parquet::ConvertedType::NONE);
    else if(type == KC || type == KS || (20 <= type && type <= 76))
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
    else if(type == KP || type == KN)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT96, parquet::ConvertedType::NONE);
    else if(type == KD)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::DATE);
    else if(type == KT)
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::TIME_MILLIS);
    else
        return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
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
    else if(type == KJ)
        writeCol(static_cast<parquet::Int64Writer*>(rg_writer->NextColumn()), col->n, &kJ64(col)[0]);
    else if(type == KE)
        writeCol(static_cast<parquet::FloatWriter*>(rg_writer->NextColumn()), col->n, &kE(col)[0]);
    else if(type == KF || type == KZ)
        writeCol(static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn()), col->n, &kF(col)[0]);
    else if(type == KC)
        writeCharCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
    else if(type == KS)
        writeSymCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
    else if(type == KP || type == KN)
        writeCol(static_cast<parquet::Int96Writer*>(rg_writer->NextColumn()), col);
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
void WRITER::writeGuidCol(parquet::FixedLenByteArrayWriter* writer, K col){
    for (int i = 0; i < col->n; i++) {
        parquet::FixedLenByteArray value(kU(col)[i].g);
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}
#endif

void WRITER::writeByteCol(parquet::FixedLenByteArrayWriter* writer, K col){
    for (int i = 0; i < col->n; i++) {
        parquet::FixedLenByteArray value(&kG(col)[i]);
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}

void WRITER::writeShortCol(parquet::Int32Writer* writer, K col){
    for (int i = 0; i < col->n; i++)
        writer->WriteBatch(1, nullptr, nullptr, reinterpret_cast<int32_t*>(&kH(col)[i]));
}

void WRITER::writeCol(parquet::Int96Writer* writer, K col){
    for (int i = 0; i < col->n; i++){
        parquet::Int96 value;
        //Magic number that adjusts for julian days
        value.value[2]=2451545;
        parquet::Int96SetNanoSeconds(value,kJ(col)[i]);
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}

void WRITER::writeCharCol(parquet::ByteArrayWriter* writer, K col){
    for (int i = 0; i < col->n; i++) {
        parquet::ByteArray value(1,&kG(col)[i]);
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}

void WRITER::writeSymCol(parquet::ByteArrayWriter* writer, K col){
    for (int i = 0; i < col->n; i++) {
        parquet::ByteArray value(std::string(kS(col)[i]).length(),reinterpret_cast<unsigned char*>(kS(col)[i]));
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}

void WRITER::writeCol(parquet::ByteArrayWriter* writer, K col){
    for (int i = 0; i < col->n; i++) {
        parquet::ByteArray value(kK(col)[i]->n,kG(kK(col)[i]));
        writer->WriteBatch(1, nullptr, nullptr, &value);
    }
}