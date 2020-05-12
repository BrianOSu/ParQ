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
                static K p2kType(std::shared_ptr<parquet::ColumnReader> column_reader, int len);
                static void readColumns(K col, std::shared_ptr<parquet::ColumnReader> column_reader,
                                   int rowCount);
                                   
                template<typename T, typename F> 
                static void getCol(K col, T *reader, int rowCount, F func);

                static void getBoolCol(K col, parquet::BoolReader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getIntCol(K col, parquet::Int32Reader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getDateCol(K col, parquet::Int32Reader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getShortCol(K col, parquet::Int32Reader *reader, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static std::vector<int32_t> extractShorts(parquet::Int32Reader *reader, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getLongCol(K col, parquet::Int64Reader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getTimestampCol(K col, parquet::Int64Reader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getInt96Col(K col, parquet::Int96Reader *reader, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getFloatCol(K col, parquet::FloatReader *reader, int rowCount, 
                                     std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getDoubleCol(K col, parquet::DoubleReader *reader, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getByteCol(K col, parquet::ByteArrayReader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getSymCol(K col, parquet::ByteArrayReader *reader, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                static void getFLBACol(K col, parquet::FixedLenByteArrayReader *reader, int rowCount, 
                                      std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                #if KXVER>=3
                static void getUUIDCol(K col, parquet::FixedLenByteArrayReader *reader, int rowCount, 
                                    std::vector<uint8_t> valid_bits, int64_t null_count, int64_t levels_read);
                #endif
            private:
                PREADER(const PREADER&) = delete;
                void operator=(const PREADER&) = delete;
        };
    }
}
#endif