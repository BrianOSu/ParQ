#include <reader.hpp>

using namespace KDB::PARQ;

std::shared_ptr<parquet::ParquetFileReader> PREADER::open_reader(const std::string& path){
    return parquet::ParquetFileReader::OpenFile(path, false);
}

K PREADER::readColumns(std::shared_ptr<parquet::ColumnReader> column_reader,
                    int rowCount,
                    int fixedLengthByteSize){
    switch(column_reader->type()){
        case Type::BOOLEAN:
            return getCol(static_cast<parquet::BoolReader*>(column_reader.get()), rowCount);
        case Type::INT32:
            switch(column_reader->descr()->converted_type()){
                case parquet::ConvertedType::TIME_MILLIS:
                    return getTimeCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount);
                case parquet::ConvertedType::DATE:
                    return getDateCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount);
                case parquet::ConvertedType::INT_16:
                    return getShortCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount);
                default:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount);
            }
        case Type::INT64:
            return getCol(static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount);
        case Type::INT96:
            return getCol(static_cast<parquet::Int96Reader*>(column_reader.get()), rowCount);
        case Type::FLOAT:
            return getCol(static_cast<parquet::FloatReader*>(column_reader.get()), rowCount);
        case Type::DOUBLE:
            return getCol(static_cast<parquet::DoubleReader*>(column_reader.get()), rowCount);
        case Type::BYTE_ARRAY:
            switch(column_reader->descr()->converted_type()){
                case parquet::ConvertedType::UTF8:
                    return getStringCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount);
                default:
                    return getCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount);
            }
        case Type::FIXED_LEN_BYTE_ARRAY:
            return getCol(static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get()), rowCount, fixedLengthByteSize);
        default:return nullptr;
    }
}

K PREADER::getCol(parquet::BoolReader *reader, int rowCount){
    K res = ktn(KB,rowCount);
    uint8_t valid_bits[rowCount+1];
    int64_t values_read;
    int64_t null_count = -1;
    int64_t levels_read = 0;
    reader->ReadBatchSpaced(rowCount, nullptr, nullptr, &kB(res)[0], valid_bits, 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getCol(parquet::Int32Reader *reader, int rowCount){
    K res = ktn(KI,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++)
        reader->ReadBatchSpaced(1, nullptr, nullptr, &kI(res)[i], valid_bits.data(), 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getTimeCol(parquet::Int32Reader *reader, int rowCount){
    K res = ktn(KT,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++)
        reader->ReadBatchSpaced(1, nullptr, nullptr, &kI(res)[i], valid_bits.data(), 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getDateCol(parquet::Int32Reader *reader, int rowCount){
    K res = ktn(KD,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++)
        reader->ReadBatchSpaced(1, nullptr, nullptr, &kI(res)[i], valid_bits.data(), 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getShortCol(parquet::Int32Reader *reader, int rowCount){
    K res = ktn(KH,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    int32_t value;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, nullptr, nullptr, &value, valid_bits.data(), 0, &levels_read, nullptr, &null_count);
        kH(res)[i]=value;
    }
    return res;
}

K PREADER::getCol(parquet::Int64Reader *reader, int rowCount){
    K res = ktn(KJ,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++)
        reader->ReadBatchSpaced(1, nullptr, nullptr, &kJ64(res)[i], valid_bits.data(), 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getCol(parquet::Int96Reader *reader, int rowCount){
    //magic number convert julian date to unix epoch
    int64_t unixTime=946684800000000000;
    K res = ktn(KP,rowCount);
    parquet::Int96 value;
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, nullptr, nullptr, &value, valid_bits.data(), 0, &levels_read, nullptr, &null_count);
        kJ(res)[i]=parquet::Int96GetNanoSeconds(value)-unixTime;
    }
    return res;
}

K PREADER::getCol(parquet::FloatReader *reader, int rowCount){
    K res = ktn(KE,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++)
        reader->ReadBatchSpaced(1, nullptr, nullptr, &kE(res)[i], valid_bits.data(), 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getCol(parquet::DoubleReader *reader, int rowCount){
    K res = ktn(KF,rowCount);
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++)
        reader->ReadBatchSpaced(1, nullptr, nullptr, &kF(res)[i], valid_bits.data(), 0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getCol(parquet::ByteArrayReader *reader, int rowCount){
    K res = ktn(0,0);
    parquet::ByteArray value;
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, nullptr, nullptr, &value, valid_bits.data(), 0, &levels_read, nullptr, &null_count);
        K bytes = ktn(KG, value.len); 
        std::copy(&value.ptr[0], &value.ptr[0]+value.len, kG(bytes));
        jk(&res,bytes);
    }
    return res;
}

K PREADER::getStringCol(parquet::ByteArrayReader *reader, int rowCount){
    K res = ktn(0,0);
    parquet::ByteArray value;
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, nullptr, nullptr, &value, valid_bits.data(), 0, &levels_read, nullptr, &null_count);
        jk(&res,kpn((char*)&value.ptr[0], value.len));
    }
    return res;
}

K PREADER::getCol(parquet::FixedLenByteArrayReader *reader, int rowCount, int fixedLengthByteSize){
    K res = ktn(0,0);
    parquet::FLBA value;
    std::vector<uint8_t> valid_bits(rowCount, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, nullptr, nullptr, &value, valid_bits.data(), 0, &levels_read, nullptr, &null_count);
        K bytes = ktn(KG, fixedLengthByteSize);
        std::copy(&value.ptr[0], &value.ptr[0]+fixedLengthByteSize, kG(bytes));
        jk(&res,bytes);
    }
    return res;
}