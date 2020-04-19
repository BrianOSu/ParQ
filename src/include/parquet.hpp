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

#ifndef KDB_PARQUET
#define KDB_PARQUET

#include <reader.hpp>
#include <writer.hpp>

namespace KDB{
    namespace PARQ{
        class PKDB{
            public:
                PKDB(std::shared_ptr<parquet::ParquetFileReader> filerReader);
                ~PKDB();

                static PKDB& getInstance(){return *instance;};
                static K loadReader(std::string fileName);
                static void updateMetaData();
                static K readGroup(std::string fileName, int group, K cols);
                static K readTable(std::shared_ptr<parquet::RowGroupReader> row_group_reader, 
                                    int num_cols,
                                    int num_rows,
                                    K cols);
                static int getColIndex(std::shared_ptr<parquet::RowGroupReader> row_group_reader, std::string colName);
                static S readColName(std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index);
                static K getColData(std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index, int num_rows);
                static K close();
                static void incrementCurrentRowGroup(){instance->currentRowGroup++;};

                std::shared_ptr<parquet::ParquetFileReader> filerReader_;
                std::shared_ptr<parquet::RowGroupReader> row_group_reader;
                const parquet::RowGroupMetaData* metaData;
                int numColumns;
                int numRows;
                int totalRowGroups;
                int currentRowGroup;
            private:
                PKDB(const PKDB&) = delete;
                void operator=(const PKDB&) = delete;
                
                static PKDB* instance;
                
        };

        class PWRITE{
            public:
                PWRITE(std::shared_ptr<parquet::ParquetFileWriter> fileWriter);
                ~PWRITE();

                static PWRITE& getInstance(){return *instance;};
                static std::shared_ptr<parquet::ParquetFileWriter> open_file_writer(K &colNames, 
                                                                               K &colValues, 
                                                                               std::string fileName,
                                                                               bool single,
                                                                               parquet::Compression::type codec,
                                                                               bool append);
                static K write(K &table, std::string fileName, bool single,
                               parquet::Compression::type codec, bool append);
                static K close();

                std::shared_ptr<GroupNode> schema_;
                std::shared_ptr<parquet::ParquetFileWriter> fileWriter_;
                int currentRowGroup;
                
            private:
                PWRITE(const PWRITE&) = delete;
                void operator=(const PWRITE&) = delete;
                
                static PWRITE* instance;
        };
    }
}
#endif