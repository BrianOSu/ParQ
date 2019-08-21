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
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KB,rowCount);
    bool value;
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kB(res)[i], &values_read)){
            if(values_read==0){
                ja(&res,0x00);
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::Int32Reader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KI,rowCount);
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kI(res)[i], &values_read)){
            if(values_read==0){
                kI(res)[i]=ni;
            }
        }
    }
    return res;
}

K PREADER::getTimeCol(parquet::Int32Reader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KT,rowCount);
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kI(res)[i], &values_read)){
            if(values_read==0){
                kI(res)[i]=ni;
            }
        }
    }
    return res;
}

K PREADER::getDateCol(parquet::Int32Reader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KD,rowCount);
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kI(res)[i], &values_read)){
            if(values_read==0){
                kI(res)[i]=ni;
            }
        }
    }
    return res;
}

K PREADER::getShortCol(parquet::Int32Reader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KH,rowCount);
    for(int i=0;i<rowCount;i++){
        int32_t value;
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &value, &values_read)){
            if(values_read==1){
                kH(res)[i]=value;
            }else{
                kH(res)[i]=nh;
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::Int64Reader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KJ,rowCount);
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kJ64(res)[i], &values_read)){
            if(values_read==0){
                ja(&res,kj(nj));
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::Int96Reader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    //magic number convert julian date to unix epoch
    int64_t unixTime=946684800000000000;
    K res = ktn(KP,rowCount);
    parquet::Int96 value;
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &value, &values_read)){
            if(values_read==1){
                kJ(res)[i]=parquet::Int96GetNanoSeconds(value)-unixTime;
            }
            else{
                kJ(res)[i]=nj;
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::FloatReader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KE,rowCount);
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kE(res)[i], &values_read)){
            if(values_read==0){
                kE(res)[i]=nf;
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::DoubleReader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(KF,rowCount);
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &kF(res)[i], &values_read)){
            if(values_read==0){
                kF(res)[i]=nf;
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::ByteArrayReader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(0,0);
    parquet::ByteArray value;
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &value, &values_read)){
            int len = value.len;
            if(values_read==1){
                K bytes = ktn(KG, len); 
                std::copy(&value.ptr[0], &value.ptr[0]+len, kG(bytes));
                jk(&res,bytes);
            } else{
                jk(&res,ktn(KG,0));
            }
        }
    }
    return res;
}

K PREADER::getStringCol(parquet::ByteArrayReader *reader, int rowCount){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(0,0);
    parquet::ByteArray value;
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &value, &values_read)){
            if(values_read==1){
                jk(&res,kpn((char*)&value.ptr[0], value.len));
            } else{
                jk(&res,kp((char*)""));
            }
        }
    }
    return res;
}

K PREADER::getCol(parquet::FixedLenByteArrayReader *reader, int rowCount, int fixedLengthByteSize){
    int64_t values_read;
    int16_t catch_nulls=1;
    K res = ktn(0,0);
    parquet::FixedLenByteArray value;
    for(int i=0;i<rowCount;i++){
        if(reader->ReadBatch(1, &catch_nulls, nullptr, &value, &values_read)){
            if(values_read==1){
                K bytes = ktn(KG, fixedLengthByteSize);
                std::copy(&value.ptr[0], &value.ptr[0]+fixedLengthByteSize, kG(bytes));
                jk(&res,bytes);
            }
            else{
                jk(&res,ktn(KG,0));
            }
        }
    }
    return res;
}