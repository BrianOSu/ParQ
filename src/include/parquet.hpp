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
                                                                               parquet::Compression::type codec);
                static K write(K &table, std::string fileName, bool single, parquet::Compression::type codec);
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