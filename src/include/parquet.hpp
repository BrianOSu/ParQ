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
#include <memory>
#include <string>

namespace KDB{
    namespace PARQ{
        class PKDB{
            public:
                PKDB(std::shared_ptr<parquet::ParquetFileReader> filerReader);
                ~PKDB() noexcept;
                void close();

                static K readGroup(std::string fileName, int group, K cols);
                static K readTable(std::shared_ptr<parquet::RowGroupReader> row_group_reader, 
                                    int num_cols,
                                    int num_rows,
                                    K cols);
                static int getColIndex(std::shared_ptr<parquet::RowGroupReader> row_group_reader, std::string colName);
                static S readColName(std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index);
                static K getColData(std::shared_ptr<parquet::RowGroupReader> row_group_reader, int index, int num_rows);
                K readCurrentGroup(K cols) const;
                bool advanceRowGroup();
                std::string schema() const;
                std::shared_ptr<const parquet::KeyValueMetadata> keyValueMetadata() const;
                int currentRowGroup() const noexcept { return currentRowGroup_; }
                int totalRowGroups() const noexcept { return totalRowGroups_; }
            private:
                PKDB(const PKDB&) = delete;
                PKDB& operator=(const PKDB&) = delete;

                void updateMetaData();
                void closeFile();

                std::shared_ptr<parquet::ParquetFileReader> filerReader_;
                std::shared_ptr<const parquet::KeyValueMetadata> key_value_metadata_;
                std::shared_ptr<parquet::RowGroupReader> row_group_reader_;
                const parquet::RowGroupMetaData* metaData_ = nullptr;
                int numColumns_ = 0;
                int numRows_ = 0;
                int totalRowGroups_ = 0;
                int currentRowGroup_ = 0;
                bool closed_ = false;
        };

        class PWRITE{
            public:
                PWRITE(std::shared_ptr<parquet::ParquetFileWriter> fileWriter);
                ~PWRITE() noexcept;

                static PWRITE* getInstance() noexcept { return instance.get(); }
                static std::shared_ptr<parquet::ParquetFileWriter> open_file_writer(K colNames, 
                                                                                    K colValues, 
                                                                                    std::string fileName,
                                                                                    bool single,
                                                                                    parquet::Compression::type codec,
                                                                                    bool append,
                                                                                    K metadata);
                static K write(K table, std::string fileName, bool single,
                               parquet::Compression::type codec, bool append, K metadata);
                static K close();

            private:
                PWRITE(const PWRITE&) = delete;
                void operator=(const PWRITE&) = delete;

                void closeFile();
                std::shared_ptr<parquet::ParquetFileWriter> fileWriter_;
                int currentRowGroup_ = 0;
                bool closed_ = false;
                static std::unique_ptr<PWRITE> instance;
        };
    }
}
#endif
