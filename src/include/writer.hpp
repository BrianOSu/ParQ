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

#ifndef KDB_PARQUET_WRITER
#define KDB_PARQUET_WRITER

#include <utils.hpp>
#include <arrow/io/file.h>
#include <arrow/util/key_value_metadata.h>
#include <parquet/api/writer.h>

using parquet::LogicalType;
using parquet::Repetition;
using parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::PrimitiveNode;

namespace KDB{
    namespace PARQ{
        class WRITER{
            public:
                static std::shared_ptr<arrow::KeyValueMetadata> KeyValueMetadata(K metadata);
                static std::shared_ptr<parquet::ParquetFileWriter> OpenFile(std::string fileName,
                                                                            std::shared_ptr<GroupNode> schema, 
                                                                            parquet::Compression::type codec, 
                                                                            bool append, 
                                                                            K metadata);
                static std::shared_ptr<GroupNode> SetupSchema(K names, K values, int numCols);
                static parquet::schema::NodePtr k2parquet(const std::string& name, int type, int firstType);

                template<typename T, typename T1>static void writeCol(T writer, int len, T1 col);
                //static void writeCol(parquet::BoolWriter* writer, int len, B* col);
                #if KXVER>=3
                static void writeGuidCol(parquet::FixedLenByteArrayWriter* writer, K col);
                #endif
                static void writeByteCol(parquet::FixedLenByteArrayWriter* writer, K col);
                static void writeDateCol(parquet::Int32Writer* writer, K col);
                static void writeShortCol(parquet::Int32Writer* writer, K col);
                static void writeTimestampCol(parquet::Int64Writer* writer, K col);
                static void writeCol(parquet::Int96Writer* writer, K col);
                static void writeCharCol(parquet::ByteArrayWriter* writer, K col);
                static void writeSymCol(parquet::ByteArrayWriter* writer, K col);
                static void writeCol(parquet::ByteArrayWriter* writer, K col);
                static void writeColumn(K col, parquet::RowGroupWriter* rg_writer);
                static std::vector<parquet::ByteArray> byteToVec(K col);
                static std::vector<parquet::ByteArray> stringToVec(K col);
                static std::vector<parquet::ByteArray> kToVec(K col);
        };
    }
}
#endif