#ifndef KDB_PARQUET_READER
#define KDB_PARQUET_READER

#include <parquet.hpp>

namespace KDB{
    namespace PARQ{
        class PREADER{
            public:
                PREADER();
                ~PREADER();

                static std::shared_ptr<parquet::ParquetFileReader> open_reader(const std::string& path);
                static K readColumns(std::shared_ptr<parquet::ColumnReader> column_reader,
                                   int rowCount,
                                   int fixedLengthByteSize);

                static K getCol(parquet::BoolReader *reader, int rowCount);
                static K getCol(parquet::Int32Reader *reader, int rowCount);
                static K getTimeCol(parquet::Int32Reader *reader, int rowCount);
                static K getDateCol(parquet::Int32Reader *reader, int rowCount);
                static K getShortCol(parquet::Int32Reader *reader, int rowCount);
                static K getCol(parquet::Int64Reader *reader, int rowCount);
                static K getCol(parquet::Int96Reader *reader, int rowCount);
                static K getCol(parquet::FloatReader *reader, int rowCount);
                static K getCol(parquet::DoubleReader *reader, int rowCount);
                static K getCol(parquet::ByteArrayReader *reader, int rowCount);
                static K getStringCol(parquet::ByteArrayReader *reader, int rowCount);
                static K getCol(parquet::FixedLenByteArrayReader *reader, int rowCount, int fixedLengthByteSize);
            private:
                PREADER(const PREADER&) = delete;
                void operator=(const PREADER&) = delete;
        };
    }
}
#endif