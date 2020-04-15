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
            return getCol(static_cast<parquet::BoolReader*>(column_reader.get()), rowCount, KB, getBoolCol);
        case Type::INT32:
            switch(column_reader->descr()->converted_type()){
                case parquet::ConvertedType::TIME_MILLIS:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KT, getIntCol);
                case parquet::ConvertedType::DATE:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KD, getIntCol);
                case parquet::ConvertedType::INT_16:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KH, getShortCol);
                default:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KI, getIntCol);
            }
        case Type::INT64:
            return getCol(static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, KJ, getLongCol);
        case Type::INT96:
            return getCol(static_cast<parquet::Int96Reader*>(column_reader.get()), rowCount, KP, getInt96Col);
        case Type::FLOAT:
            return getCol(static_cast<parquet::FloatReader*>(column_reader.get()), rowCount, KE, getFloatCol);
        case Type::DOUBLE:
            return getCol(static_cast<parquet::DoubleReader*>(column_reader.get()), rowCount, KF, getDoubleCol);
        case Type::BYTE_ARRAY:
            switch(column_reader->descr()->converted_type()){
                case parquet::ConvertedType::UTF8:
                    return getCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, 0, getStringCol);
                default:
                    return getCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, 0, getByteCol);
            }
        case Type::FIXED_LEN_BYTE_ARRAY:
            return getCol(static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get()), rowCount, fixedLengthByteSize);
        default:return nullptr;
    }
}

template<typename T, typename F>
K PREADER::getCol(T *reader, int rowCount, int kType, F func){
    std::vector<uint8_t> valid_bits(rowCount+1, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    return func(reader, kType, rowCount, valid_bits, null_count, levels_read);
}

K PREADER::getBoolCol(parquet::BoolReader *reader, int kType, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level,
                                             &kB(res)[rows_read], valid_bits.data()+rows_read,
                                             0, &levels_read, nullptr, &null_count);
    return res;
}

K PREADER::getIntCol(parquet::Int32Reader *reader, int kType, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &kI(res)[rows_read], valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
    return res;
}

K PREADER::getShortCol(parquet::Int32Reader *reader, int kType, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType, rowCount);
    int32_t value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        kH(res)[i]=value;
    }
    return res;
}

K PREADER::getLongCol(parquet::Int64Reader *reader, int kType, int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level,
                                             &kJ64(res)[rows_read], valid_bits.data()+rows_read,
                                             0, &levels_read, &values_read, &null_count);
    return res;
}

K PREADER::getInt96Col(parquet::Int96Reader *reader, int kType, int rowCount, 
                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    //magic number convert julian date to unix epoch
    int64_t unixTime=946684800000000000;
    K res = ktn(kType, rowCount);
    parquet::Int96 value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        kJ(res)[i]=parquet::Int96GetNanoSeconds(value)-unixTime;
    }
    return res;
}

K PREADER::getFloatCol(parquet::FloatReader *reader, int kType, int rowCount, 
                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &kE(res)[rows_read], valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
    return res;
}

K PREADER::getDoubleCol(parquet::DoubleReader *reader, int kType, int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType,rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &kF(res)[rows_read], valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
    return res;
}

K PREADER::getByteCol(parquet::ByteArrayReader *reader, int kType, int rowCount, 
                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(kType, 0);
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        K bytes = ktn(KG, value.len); 
        std::copy(&value.ptr[0], &value.ptr[0]+value.len, kG(bytes));
        jk(&res,bytes);
    }
    return res;
}

K PREADER::getStringCol(parquet::ByteArrayReader *reader, int kType, int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(0,0);
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        jk(&res,kpn((char*)&value.ptr[0], value.len));
    }
    return res;
}

K PREADER::getCol(parquet::FixedLenByteArrayReader *reader, int rowCount, int fixedLengthByteSize){
    K res = ktn(0,0);
    parquet::FLBA value;
    std::vector<uint8_t> valid_bits(rowCount+1, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        K bytes = ktn(KG, fixedLengthByteSize);
        std::copy(&value.ptr[0], &value.ptr[0]+fixedLengthByteSize, kG(bytes));
        jk(&res,bytes);
    }
    return res;
}