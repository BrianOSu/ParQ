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

void PREADER::readColumns(K col, std::shared_ptr<parquet::ColumnReader> column_reader,
                        int rowCount ){
    switch(column_reader->type()){
        case Type::BOOLEAN:
            getCol(col, static_cast<parquet::BoolReader*>(column_reader.get()), rowCount, getBoolCol); break;
        case Type::INT32:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::DATE:
                    getCol(col, static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, getDateCol); break;
                case parquet::LogicalType::Type::TIME:
                    getCol(col, static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, getIntCol); break;
                case parquet::LogicalType::Type::INT:
                    if(column_reader->descr()->logical_type()->is_compatible(parquet::ConvertedType::INT_16)){
                        getCol(col, static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, getShortCol); break;}
                default:
                    getCol(col, static_cast<parquet::Int32Reader*>(column_reader.get()), rowCount, getIntCol); break;
            } break;
        case Type::INT64:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::TIMESTAMP:
                    if(column_reader->descr()->logical_type()->ToString().find("timeUnit=nanoseconds") != std::string::npos){
                        getCol(col, static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, getTimestampCol); break;}
                case parquet::LogicalType::Type::TIME:
                    if(column_reader->descr()->logical_type()->ToString().find("timeUnit=nanoseconds") != std::string::npos){
                        getCol(col, static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, getLongCol); break;}
                default:
                    getCol(col, static_cast<parquet::Int64Reader*>(column_reader.get()), rowCount, getLongCol); break;
            } break;
        case Type::INT96:
            getCol(col, static_cast<parquet::Int96Reader*>(column_reader.get()), rowCount, getInt96Col); break;
        case Type::FLOAT:
            getCol(col, static_cast<parquet::FloatReader*>(column_reader.get()), rowCount, getFloatCol); break;
        case Type::DOUBLE:
            getCol(col, static_cast<parquet::DoubleReader*>(column_reader.get()), rowCount, getDoubleCol); break;
        case Type::BYTE_ARRAY:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::STRING:
                    getCol(col, static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, getStringCol); break;
                case parquet::LogicalType::Type::ENUM:
                    getCol(col, static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, getSymCol); break;
                default:
                    getCol(col, static_cast<parquet::ByteArrayReader*>(column_reader.get()), rowCount, getByteCol); break;
            } break;
        case Type::FIXED_LEN_BYTE_ARRAY:
            switch(column_reader->descr()->logical_type()->type()){
                #if KXVER>=3
                case parquet::LogicalType::Type::UUID:
                    getCol(col, static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get()), rowCount, getUUIDCol); break;
                #endif
                default:
                    getCol(col, static_cast<parquet::FixedLenByteArrayReader*>(column_reader.get()), rowCount, getFLBACol); break;
            }
    }
}

template<typename T, typename F>
void PREADER::getCol(K col, T *reader, int rowCount, F func){
    std::vector<uint8_t> valid_bits(rowCount+1, 255);
    int64_t null_count = -1;
    int64_t levels_read = 0;
    func(col, reader, rowCount, valid_bits, null_count, levels_read);
}

void PREADER::getBoolCol(K col, parquet::BoolReader *reader, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level,
                                             &kB(col)[rows_read], valid_bits.data()+rows_read,
                                             0, &levels_read, &values_read, &null_count);
}

void PREADER::getIntCol(K col, parquet::Int32Reader *reader, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &kI(col)[rows_read], valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
}

void PREADER::getDateCol(K col, parquet::Int32Reader *reader, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    getIntCol(col, reader, rowCount, valid_bits, null_count, levels_read);
    std::for_each(&kI(col)[0], &kI(col)[0] + rowCount, [](int &n){ n-=10957; });
}

void PREADER::getShortCol(K col, parquet::Int32Reader *reader, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    std::copy_n(extractShorts(reader, rowCount, valid_bits, null_count, levels_read).begin(), rowCount, &kH(col)[0]);
}

std::vector<int32_t> PREADER::extractShorts(parquet::Int32Reader *reader, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    std::vector<int32_t> value(rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &value[0] + rows_read, valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
    return value;
}

void PREADER::getLongCol(K col, parquet::Int64Reader *reader, int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level,
                                             &kJ64(col)[rows_read], valid_bits.data()+rows_read,
                                             0, &levels_read, &values_read, &null_count);
}

void PREADER::getTimestampCol(K col, parquet::Int64Reader *reader, int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    getLongCol(col, reader, rowCount, valid_bits, null_count, levels_read);
    std::for_each(&kJ64(col)[0], &kJ64(col)[0] + rowCount, [](int64_t &n){ n-=946684800000000000; });
}

void PREADER::getInt96Col(K col, parquet::Int96Reader *reader, int rowCount, 
                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    //magic number convert julian date to unix epoch
    int64_t unixTime=946684800000000000;
    parquet::Int96 value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        kJ(col)[i]=parquet::Int96GetNanoSeconds(value)-unixTime;
    }
}

void PREADER::getFloatCol(K col, parquet::FloatReader *reader, int rowCount, 
                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &kE(col)[rows_read], valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
}

void PREADER::getDoubleCol(K col, parquet::DoubleReader *reader, int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext())
        rows_read += reader->ReadBatchSpaced(rowCount, &definition_level, &repetition_level, 
                                             &kF(col)[rows_read], valid_bits.data()+rows_read, 
                                             0, &levels_read, &values_read, &null_count);
}

void PREADER::getByteCol(K col, parquet::ByteArrayReader *reader, int rowCount, 
                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    K res = ktn(0, 0);
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
}

void PREADER::getStringCol(K col, parquet::ByteArrayReader *reader, int rowCount, 
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
}

void PREADER::getSymCol(K col, parquet::ByteArrayReader *reader,  int rowCount, 
                        std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        kS(col)[i]=sn((char*)&value.ptr[0], value.len);
    }
}

void PREADER::getFLBACol(K col, parquet::FixedLenByteArrayReader *reader, int rowCount, 
                  std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    int size = reader->descr()->type_length();
    K res = size == 1 ? ktn(KG,rowCount) : ktn(0,0);
    parquet::FLBA value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    if(size ==1){
        for(int i=0;i<rowCount;i++){
            reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                    valid_bits.data(), 0, &levels_read, &values_read, &null_count);
            kG(res)[i]=*value.ptr;
        }
    } else {
        for(int i=0;i<rowCount;i++){
            reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                    valid_bits.data(), 0, &levels_read, &values_read, &null_count);
            K bytes = ktn(KG, size); 
            std::copy(&value.ptr[0], &value.ptr[0]+size, kG(bytes));
            jk(&res,bytes); 
        }           
    }
}

#if KXVER>=3
void PREADER::getUUIDCol(K col, parquet::FixedLenByteArrayReader *reader, int rowCount, 
                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read){
    parquet::FLBA value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatchSpaced(1, &definition_level, &repetition_level, &value, 
                                valid_bits.data(), 0, &levels_read, &values_read, &null_count);
        std::copy(&value.ptr[0], &value.ptr[0]+16, kU(col)[i].g);
    }
}
#endif

K PREADER::p2kType(std::shared_ptr<parquet::ColumnReader> column_reader, int len){
    switch(column_reader->type()){
        case Type::BOOLEAN:
            return ktn(KB, len);
        case Type::INT32:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::DATE:
                    return ktn(KD, len);
                case parquet::LogicalType::Type::TIME:
                    return ktn(KT, len);
                case parquet::LogicalType::Type::INT:
                    if(column_reader->descr()->logical_type()->is_compatible(parquet::ConvertedType::INT_16))
                        return ktn(KH, len);
                default:
                    return ktn(KI, len);
            }
        case Type::INT64:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::TIMESTAMP:
                    if(column_reader->descr()->logical_type()->ToString().find("timeUnit=nanoseconds") != std::string::npos)
                        return ktn(KP, len);
                case parquet::LogicalType::Type::TIME:
                    if(column_reader->descr()->logical_type()->ToString().find("timeUnit=nanoseconds") != std::string::npos)
                        return ktn(KN, len);
                default:
                    return ktn(KJ, len);
            }
        case Type::INT96:
            return ktn(KP, len);
        case Type::FLOAT:
            return ktn(KE, len);
        case Type::DOUBLE:
            return ktn(KF, len);
        case Type::BYTE_ARRAY:
            switch(column_reader->descr()->logical_type()->type()){
                case parquet::LogicalType::Type::STRING:
                    return ktn(0, len);
                case parquet::LogicalType::Type::ENUM:
                    return ktn(KS, len);
                default:
                    return ktn(0, len);
            }
        case Type::FIXED_LEN_BYTE_ARRAY:
            switch(column_reader->descr()->logical_type()->type()){
                #if KXVER>=3
                case parquet::LogicalType::Type::UUID:
                    return ktn(UU, len);
                #endif
                default:
                    return ktn(0, len);
            }
        default:return ktn(0, len);
    }
}