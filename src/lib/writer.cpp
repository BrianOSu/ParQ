#include <writer.hpp>

using namespace KDB::PARQ;

std::shared_ptr<parquet::ParquetFileWriter> WRITER::OpenFile(std::string fileName, std::shared_ptr<GroupNode> schema){
    using FileClass = ::arrow::io::FileOutputStream;
    std::shared_ptr<FileClass> out_file;
    FileClass::Open(fileName, &out_file);

    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::ZSTD);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    return parquet::ParquetFileWriter::Open(out_file, schema, props);
}

std::shared_ptr<GroupNode> WRITER::SetupSchema(K names, K values, int numCols){
    parquet::schema::NodeVector fields;
    for(int i=0;i<numCols;i++)
        fields.push_back(k2parquet(kS(names)[i], kK(values)[i]));
    return std::static_pointer_cast<GroupNode>(
        GroupNode::Make("schema", Repetition::REQUIRED, fields));
}

parquet::schema::NodePtr WRITER::k2parquet(const std::string& name,K type){
    switch(type->t){
        case KB:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::BOOLEAN, parquet::ConvertedType::NONE);
        case UU:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE, 16);
        case KG:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, parquet::ConvertedType::NONE, 1);
        case KH:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::INT_16);
        case KI:
        case KM:
        case KU:
        case KV:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::NONE);
        case KJ:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64, parquet::ConvertedType::INT_64);
        case KE:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FLOAT, parquet::ConvertedType::NONE);
        case KF:
        case KZ:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::DOUBLE, parquet::ConvertedType::NONE);
        case KC:
        case KS:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
        case KP:
        case KN:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT96, parquet::ConvertedType::NONE);
        case KD:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::DATE);
        case KT:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32, parquet::ConvertedType::TIME_MILLIS);
        default:
            return PrimitiveNode::Make(name, Repetition::REQUIRED, Type::BYTE_ARRAY, parquet::ConvertedType::UTF8);
    }
}

void WRITER::writeColumn(K col, parquet::RowGroupWriter* rg_writer){
    switch(col->t){
        case KB:
            writeCol(static_cast<parquet::BoolWriter*>(rg_writer->NextColumn()), col->n, &kB(col)[0]);
            break;
        #if KXVER>=3
        case UU:
            writeGuidCol(static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn()), col);
            break;
        #endif
        case KG:
            writeByteCol(static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->NextColumn()), col);
            break;
        case KH:
            writeShortCol(static_cast<parquet::Int32Writer*>(rg_writer->NextColumn()), col);
            break;
        case KI:
        case KM:
        case KD:
        case KU:
        case KV:
        case KT:
            writeCol(static_cast<parquet::Int32Writer*>(rg_writer->NextColumn()), col->n, &kI(col)[0]);
            break;
        case KJ:
            writeCol(static_cast<parquet::Int64Writer*>(rg_writer->NextColumn()), col->n, &kJ64(col)[0]);
            break;
        case KE:
            writeCol(static_cast<parquet::FloatWriter*>(rg_writer->NextColumn()), col->n, &kE(col)[0]);
            break;
        case KF:
        case KZ:
            writeCol(static_cast<parquet::DoubleWriter*>(rg_writer->NextColumn()), col->n, &kF(col)[0]);
            break;
        case KC:
            writeCharCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
            break;
        case KS:
            writeSymCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
            break;
        case KP:
        case KN:
            writeCol(static_cast<parquet::Int96Writer*>(rg_writer->NextColumn()), col);
            break;
        default:
            writeCol(static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn()), col);
            break;
    }
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