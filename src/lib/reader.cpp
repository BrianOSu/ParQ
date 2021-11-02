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

#include <reader.hpp>

using namespace KDB::PARQ;

std::shared_ptr<parquet::ParquetFileReader> PREADER::open_reader(const std::string& path){
    return parquet::ParquetFileReader::OpenFile(path, false);
}

K PREADER::readColumns(std::shared_ptr<parquet::ColumnReader> column_reader,
                        int rowCount ){
    switch(column_reader->type()){
        case Type::BOOLEAN:
            return getCol(static_cast<parquet::BoolReader*>(column_reader.get()), rowCount, KB, getBoolCol);
        case Type::INT32:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::DATE:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KD, getDateCol);
                case parquet::LogicalType::Type::TIME:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KT, getIntCol);
                case parquet::LogicalType::Type::INT:
                    if(column_reader->descr()->logical_type()->is_compatible(parquet::ConvertedType::INT_16))
                        return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KH, getShortCol);
                default:
                    return getCol(static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, KI, getIntCol);
            }
        case Type::INT64:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::TIMESTAMP:
                    if(column_reader->descr()->logical_type()->ToString().find("timeUnit=nanoseconds") != std::string::npos)
                        return getCol(static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, KP, getTimestampCol);
                case parquet::LogicalType::Type::TIME:
                    if(column_reader->descr()->logical_type()->ToString().find("timeUnit=nanoseconds") != std::string::npos)
                        return getCol(static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, KN, getLongCol);
                default:
                    return getCol(static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, KJ, getLongCol);
            }
        case Type::INT96:
            return getCol(static_cast<parquet::Int96Reader*>(column_reader.get()), rowCount, KP, getInt96Col);
        case Type::FLOAT:
            return getCol(static_cast<parquet::FloatReader*>(column_reader.get()), rowCount, KE, getFloatCol);
        case Type::DOUBLE:
            return getCol(static_cast<parquet::DoubleReader*>(column_reader.get()), rowCount, KF, getDoubleCol);
        case Type::BYTE_ARRAY:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::STRING:
                    return getCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, 0, getStringCol);
                case parquet::LogicalType::Type::ENUM:
                    return getCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, 0, getSymCol);
                default:
                    return getCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, 0, getByteCol);
            }
        case Type::FIXED_LEN_BYTE_ARRAY:
            switch(column_reader->descr()->logical_type()->type()){
                #if KXVER>=3
                case parquet::LogicalType::Type::UUID:
                    return getCol(static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get()), rowCount, UU, getUUIDCol);
                #endif
                default:
                    return getCol(static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get()), rowCount, 0, getFLBACol);
            }
        default:return nullptr;
    }
}

template<typename T, typename F>
K PREADER::getCol(T *reader, int rowCount, int kType, F func){
    return func(reader, kType, rowCount);
}

K PREADER::getBoolCol(parquet::BoolReader *reader, int kType, int rowCount){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatch(rowCount, &definition_level, &repetition_level,
                                             &kB(res)[rows_read], &values_read);
    return res;
}

K PREADER::getIntCol(parquet::Int32Reader *reader, int kType, int rowCount){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatch(rowCount, &definition_level, &repetition_level, 
                                             &kI(res)[rows_read], &values_read);
    return res;
}

K PREADER::getDateCol(parquet::Int32Reader *reader, int kType, int rowCount){
    K res = getIntCol(reader, kType, rowCount);
    std::for_each(&kI(res)[0], &kI(res)[0] + rowCount, [](int &n){ n-=10957; });
    return res;
}

K PREADER::getShortCol(parquet::Int32Reader *reader, int kType, int rowCount){
    K res = ktn(KH, rowCount);
    std::copy_n(extractShorts(reader, rowCount).begin(), rowCount, &kH(res)[0]);
    return res;
}

std::vector<int32_t> PREADER::extractShorts(parquet::Int32Reader *reader, int rowCount){
    std::vector<int32_t> value(rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatch(rowCount, &definition_level, &repetition_level, 
                                             &value[0] + rows_read, &values_read);
    return value;
}

K PREADER::getLongCol(parquet::Int64Reader *reader, int kType, int rowCount){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatch(rowCount, &definition_level, &repetition_level,
                                             &kJ64(res)[rows_read], &values_read);
    return res;
}

K PREADER::getTimestampCol(parquet::Int64Reader *reader, int kType, int rowCount){
    K res = getLongCol(reader, kType, rowCount);
    std::for_each(&kJ64(res)[0], &kJ64(res)[0] + rowCount, [](int64_t &n){ n-=946684800000000000; });
    return res;
}

K PREADER::getInt96Col(parquet::Int96Reader *reader, int kType, int rowCount){
    //magic number convert julian date to unix epoch
    int64_t unixTime=946684800000000000;
    K res = ktn(kType, rowCount);
    parquet::Int96 value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        kJ(res)[i]=parquet::Int96GetNanoSeconds(value)-unixTime;
    }
    return res;
}

K PREADER::getFloatCol(parquet::FloatReader *reader, int kType, int rowCount){
    K res = ktn(kType, rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatch(rowCount, &definition_level, &repetition_level, 
                                             &kE(res)[rows_read], &values_read);
    return res;
}

K PREADER::getDoubleCol(parquet::DoubleReader *reader, int kType, int rowCount){
    K res = ktn(kType,rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatch(rowCount, &definition_level, &repetition_level, 
                                             &kF(res)[rows_read], &values_read);
    return res;
}

K PREADER::getByteCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    K res = ktn(kType, 0);
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        K bytes = ktn(KG, value.len); 
        std::copy(&value.ptr[0], &value.ptr[0]+value.len, kG(bytes));
        jk(&res,bytes);
    }
    return res;
}

K PREADER::getStringCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    K res = ktn(0,0);
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        jk(&res,kpn((char*)&value.ptr[0], value.len));
    }
    return res;
}

K PREADER::getSymCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    K res = ktn(KS,rowCount);
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        kS(res)[i]=sn((char*)&value.ptr[0], value.len);
    }
    return res;
}

K PREADER::getFLBACol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount){
    int size = reader->descr()->type_length();
    K res = size == 1 ? ktn(KG,rowCount) : ktn(0,0);
    parquet::FLBA value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    if(size ==1){
        for(int i=0;i<rowCount;i++){
            reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
            kG(res)[i]=*value.ptr;
        }
    } else {
        for(int i=0;i<rowCount;i++){
            reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
            K bytes = ktn(KG, size); 
            std::copy(&value.ptr[0], &value.ptr[0]+size, kG(bytes));
            jk(&res,bytes); 
        }           
    }
    return res;
}

#if KXVER>=3
K PREADER::getUUIDCol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount){
    K res = ktn(kType,rowCount);
    parquet::FLBA value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        std::copy(&value.ptr[0], &value.ptr[0]+16, kU(res)[i].g);
    }
    return res;
}
#endif