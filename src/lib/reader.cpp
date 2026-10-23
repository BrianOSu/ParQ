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
#include <stdexcept>

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
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q boolean column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kB(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getIntCol(parquet::Int32Reader *reader, int kType, int rowCount){
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q integer column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kI(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getDateCol(parquet::Int32Reader *reader, int kType, int rowCount){
    KOwner res{getIntCol(reader, kType, rowCount)};
    if (rowCount > 0) {
        std::for_each(kI(res.get()), kI(res.get()) + rowCount, [](int &n){ n-=10957; });
    }
    return res.release();
}

K PREADER::getShortCol(parquet::Int32Reader *reader, int kType, int rowCount){
    KOwner res{ktn(KH, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q short column");
    const auto values = extractShorts(reader, rowCount);
    std::copy_n(values.begin(), rowCount, kH(res.get()));
    return res.release();
}

std::vector<int32_t> PREADER::extractShorts(parquet::Int32Reader *reader, int rowCount){
    std::vector<int32_t> value(rowCount);
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, value.data() + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return value;
}

K PREADER::getLongCol(parquet::Int64Reader *reader, int kType, int rowCount){
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q long column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kJ64(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getTimestampCol(parquet::Int64Reader *reader, int kType, int rowCount){
    KOwner res{getLongCol(reader, kType, rowCount)};
    if (rowCount > 0) {
        std::for_each(kJ64(res.get()), kJ64(res.get()) + rowCount,
                      [](int64_t &n){ n-=946684800000000000; });
    }
    return res.release();
}

K PREADER::getInt96Col(parquet::Int96Reader *reader, int kType, int rowCount){
    //magic number convert julian date to unix epoch
    int64_t unixTime=946684800000000000;
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q timestamp column");
    parquet::Int96 value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        kJ(res.get())[i]=parquet::Int96GetNanoSeconds(value)-unixTime;
    }
    return res.release();
}

K PREADER::getFloatCol(parquet::FloatReader *reader, int kType, int rowCount){
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q real column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kE(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getDoubleCol(parquet::DoubleReader *reader, int kType, int rowCount){
    KOwner res{ktn(kType,rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q float column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read=0;
    while(reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kF(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getByteCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    KOwner res{ktn(kType, 0)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q byte column");
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        KOwner bytes{ktn(KG, value.len)};
        if (!bytes.get()) throw std::runtime_error("Failed to allocate q byte value");
        if (value.len) std::copy(value.ptr, value.ptr + value.len, kG(bytes.get()));
        jk(res.address(), bytes.release());
    }
    return res.release();
}

K PREADER::getStringCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    KOwner res{ktn(0,0)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q string column");
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        char* bytes = value.len
            ? reinterpret_cast<char*>(const_cast<uint8_t*>(value.ptr))
            : const_cast<char*>("");
        KOwner string{kpn(bytes, value.len)};
        if (!string.get()) throw std::runtime_error("Failed to allocate q string value");
        jk(res.address(), string.release());
    }
    return res.release();
}

K PREADER::getSymCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    KOwner res{ktn(KS,rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q symbol column");
    parquet::ByteArray value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        char* text = value.len
            ? reinterpret_cast<char*>(const_cast<uint8_t*>(value.ptr))
            : const_cast<char*>("");
        kS(res.get())[i]=sn(text, value.len);
    }
    return res.release();
}

K PREADER::getFLBACol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount){
    int size = reader->descr()->type_length();
    KOwner res{size == 1 ? ktn(KG,rowCount) : ktn(0,0)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q fixed byte array column");
    parquet::FLBA value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    if(size ==1){
        for(int i=0;i<rowCount;i++){
            reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
            if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
            kG(res.get())[i]=*value.ptr;
        }
    } else {
        for(int i=0;i<rowCount;i++){
            reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
            if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
            KOwner bytes{ktn(KG, size)};
            if (!bytes.get()) throw std::runtime_error("Failed to allocate q fixed byte value");
            std::copy(value.ptr, value.ptr + size, kG(bytes.get()));
            jk(res.address(), bytes.release());
        }           
    }
    return res.release();
}

#if KXVER>=3
K PREADER::getUUIDCol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount){
    KOwner res{ktn(kType,rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q UUID column");
    parquet::FLBA value;
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    for(int i=0;i<rowCount;i++){
        reader->ReadBatch(1, &definition_level, &repetition_level, &value, &values_read);
        if (values_read != 1) throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        std::copy(value.ptr, value.ptr + 16, kU(res.get())[i].g);
    }
    return res.release();
}
#endif
