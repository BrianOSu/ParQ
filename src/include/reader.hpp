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

#ifndef KDB_PARQUET_READER
#define KDB_PARQUET_READER

#include <utils.hpp>
#include <parquet/api/reader.h>

using parquet::LogicalType;
using parquet::Type;

namespace KDB{
    namespace PARQ{
        class PREADER{
            public:
                PREADER();
                ~PREADER();

                static std::shared_ptr<parquet::ParquetFileReader> open_reader(const std::string& path);
                static K readColumns(std::shared_ptr<parquet::ColumnReader> column_reader,
                                   int rowCount);

                template<typename T, typename F> 
                static K getCol(T *reader, int rowCount, int kType, F func);

                static K getBoolCol(parquet::BoolReader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getIntCol(parquet::Int32Reader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getDateCol(parquet::Int32Reader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getShortCol(parquet::Int32Reader *reader, int kType, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static std::vector<int32_t> extractShorts(parquet::Int32Reader *reader, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getLongCol(parquet::Int64Reader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getTimestampCol(parquet::Int64Reader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getInt96Col(parquet::Int96Reader *reader, int kType, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getFloatCol(parquet::FloatReader *reader, int kType, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getDoubleCol(parquet::DoubleReader *reader, int kType, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getByteCol(parquet::ByteArrayReader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getStringCol(parquet::ByteArrayReader *reader, int kType, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getSymCol(parquet::ByteArrayReader *reader, int kType, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static K getFLBACol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                #if KXVER>=3
                static K getUUIDCol(parquet::FixedLenByteArrayReader *reader, int kType, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                #endif
            private:
                PREADER(const PREADER&) = delete;
                void operator=(const PREADER&) = delete;
        };
    }
}
#endif