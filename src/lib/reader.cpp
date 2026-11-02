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

#include <array>
#include <cstring>
#include <reader.hpp>
#include <stdexcept>

using namespace KDB::PARQ;

std::shared_ptr<parquet::ParquetFileReader> PREADER::open_reader(const std::string& path){
    return parquet::ParquetFileReader::OpenFile(path, false);
}

K PREADER::readColumns(std::shared_ptr<parquet::ColumnReader> column_reader,
                        int rowCount,
                        bool stableDictionaryValues){
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
                    return getSymCol(static_cast<parquet::ByteArrayReader*>(column_reader.get()),
                                     0, rowCount, stableDictionaryValues);
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
    constexpr int32_t epoch_offset = 10957;
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q date column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kI(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            kI(res.get())[rows_read + i] -= epoch_offset;
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getShortCol(parquet::Int32Reader *reader, int kType, int rowCount){
    KOwner res{ktn(KH, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q short column");
    // Parquet's INT32 reader requires int32_t output, while q shorts are
    // int16_t. Decode bounded batches so the conversion does not need a
    // rowCount-sized temporary vector.
    constexpr int batch_capacity = 8192;
    std::vector<int32_t> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            kH(res.get())[rows_read + i] = static_cast<int16_t>(values[i]);
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
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
    constexpr int64_t epoch_offset = 946684800000000000;
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q timestamp column");
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const auto batch_rows = reader->ReadBatch(rowCount - rows_read, &definition_level,
            &repetition_level, kJ64(res.get()) + rows_read, &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            kJ64(res.get())[rows_read + i] -= epoch_offset;
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getInt96Col(parquet::Int96Reader *reader, int kType, int rowCount){
    constexpr int64_t unixTime = 946684800000000000;
    constexpr int batch_capacity = 8192;
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q timestamp column");
    std::vector<parquet::Int96> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            kJ(res.get())[rows_read + i] = parquet::Int96GetNanoSeconds(values[i]) - unixTime;
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
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
    constexpr int batch_capacity = 8192;
    KOwner res{ktn(kType, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q byte column");
    std::vector<parquet::ByteArray> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            const auto& value = values[i];
            KOwner bytes{ktn(KG, value.len)};
            if (!bytes.get()) throw std::runtime_error("Failed to allocate q byte value");
            if (value.len) std::copy(value.ptr, value.ptr + value.len, kG(bytes.get()));
            kK(res.get())[rows_read + i] = bytes.release();
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getStringCol(parquet::ByteArrayReader *reader, int kType, int rowCount){
    constexpr int batch_capacity = 8192;
    KOwner res{ktn(0, rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q string column");
    // One-character strings are common in character columns and are often
    // dictionary encoded. Reuse the q string object for each byte value rather
    // than allocating one million identical one-byte strings.
    std::array<K, 257> singleByteStrings{};
    struct StringCacheOwner {
        std::array<K, 257>& values;
        ~StringCacheOwner() {
            for (K value : values) {
                if (value) r0(value);
            }
        }
    } cacheOwner{singleByteStrings};
    std::vector<parquet::ByteArray> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            const auto& value = values[i];
            char* bytes = value.len
                ? reinterpret_cast<char*>(const_cast<uint8_t*>(value.ptr))
                : const_cast<char*>("");
            if (value.len <= 1) {
                const size_t cacheIndex = value.len == 0
                    ? 0
                    : static_cast<size_t>(value.ptr[0]) + 1;
                if (singleByteStrings[cacheIndex]) {
                    kK(res.get())[rows_read + i] = r1(singleByteStrings[cacheIndex]);
                } else {
                    KOwner string{kpn(bytes, value.len)};
                    if (!string.get()) throw std::runtime_error("Failed to allocate q string value");
                    singleByteStrings[cacheIndex] = r1(string.get());
                    kK(res.get())[rows_read + i] = string.release();
                }
                continue;
            }
            KOwner string{kpn(bytes, value.len)};
            if (!string.get()) throw std::runtime_error("Failed to allocate q string value");
            kK(res.get())[rows_read + i] = string.release();
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getSymCol(parquet::ByteArrayReader *reader,
                     int kType,
                     int rowCount,
                     bool stableDictionaryValues){
    constexpr int batch_capacity = 8192;
    constexpr size_t max_cached_symbols = 65536;
    constexpr size_t symbol_cache_capacity = 131072;
    KOwner res{ktn(KS,rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q symbol column");
    struct SymbolCacheEntry {
        const uint8_t* value = nullptr;
        S symbol = nullptr;
    };
    // Dictionary-backed byte-array pointers remain stable for the row group.
    // The caller enables this table only when metadata confirms dictionary-only data pages.
    std::vector<SymbolCacheEntry> symbols(stableDictionaryValues ? symbol_cache_capacity : 0);
    size_t cachedSymbols = 0;
    bool cacheSymbols = stableDictionaryValues;
    bool emptySymbolCached = false;
    S emptySymbol = nullptr;
    std::vector<parquet::ByteArray> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            const auto& value = values[i];
            if (cacheSymbols) {
                if (value.len == 0) {
                    if (emptySymbolCached) {
                        kS(res.get())[rows_read + i] = emptySymbol;
                    } else {
                        emptySymbol = sn(const_cast<char*>(""), 0);
                        emptySymbolCached = true;
                        kS(res.get())[rows_read + i] = emptySymbol;
                    }
                    continue;
                }

                const uintptr_t address = reinterpret_cast<uintptr_t>(value.ptr);
                uint64_t hash = address;
                hash ^= hash >> 33;
                hash *= 0xff51afd7ed558ccdULL;
                hash ^= hash >> 33;
                hash *= 0xc4ceb9fe1a85ec53ULL;
                hash ^= hash >> 33;
                size_t slot = static_cast<size_t>(hash) & (symbol_cache_capacity - 1);
                while (symbols[slot].symbol) {
                    const auto& entry = symbols[slot];
                    if (entry.value == value.ptr) {
                        kS(res.get())[rows_read + i] = entry.symbol;
                        break;
                    }
                    slot = (slot + 1) & (symbol_cache_capacity - 1);
                }
                if (symbols[slot].symbol) continue;
                if (cachedSymbols < max_cached_symbols) {
                    S symbol = sn(const_cast<char*>(reinterpret_cast<const char*>(value.ptr)), value.len);
                    symbols[slot] = {value.ptr, symbol};
                    ++cachedSymbols;
                    kS(res.get())[rows_read + i] = symbol;
                    continue;
                } else {
                    cacheSymbols = false;
                }
            }
            const char* text = value.len ? reinterpret_cast<const char*>(value.ptr) : "";
            kS(res.get())[rows_read + i] = sn(const_cast<char*>(text), value.len);
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

K PREADER::getFLBACol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount){
    constexpr int batch_capacity = 8192;
    int size = reader->descr()->type_length();
    KOwner res{size == 1 ? ktn(KG,rowCount) : ktn(0,rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q fixed byte array column");
    std::vector<parquet::FLBA> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            const auto& value = values[i];
            if (size == 1) {
                kG(res.get())[rows_read + i] = *value.ptr;
            } else {
                KOwner bytes{ktn(KG, size)};
                if (!bytes.get()) throw std::runtime_error("Failed to allocate q fixed byte value");
                std::copy(value.ptr, value.ptr + size, kG(bytes.get()));
                kK(res.get())[rows_read + i] = bytes.release();
            }
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}

#if KXVER>=3
K PREADER::getUUIDCol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount){
    constexpr int batch_capacity = 8192;
    KOwner res{ktn(kType,rowCount)};
    if (!res.get()) throw std::runtime_error("Failed to allocate q UUID column");
    std::vector<parquet::FLBA> values(std::min(rowCount, batch_capacity));
    int16_t definition_level;
    int16_t repetition_level;
    int64_t values_read;
    int rows_read = 0;
    while (reader->HasNext() && rows_read < rowCount) {
        const int batch_size = std::min(rowCount - rows_read, batch_capacity);
        const auto batch_rows = reader->ReadBatch(batch_size, &definition_level,
            &repetition_level, values.data(), &values_read);
        if (batch_rows <= 0 || values_read != batch_rows)
            throw std::runtime_error("Nullable or malformed Parquet column is unsupported");
        for (int64_t i = 0; i < batch_rows; ++i) {
            std::memcpy(kU(res.get())[rows_read + i].g, values[i].ptr, 16);
        }
        rows_read += static_cast<int>(batch_rows);
    }
    if (rows_read != rowCount) throw std::runtime_error("Parquet column has an unexpected row count");
    return res.release();
}
#endif
