#ifndef KDB_PARQUET_WRITER
#define KDB_PARQUET_WRITER

#include <utils.hpp>
#include <arrow/io/file.h>
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
                static std::shared_ptr<parquet::ParquetFileWriter> OpenFile(std::string fileName, std::shared_ptr<GroupNode> schema, parquet::Compression::type codec);
                static std::shared_ptr<GroupNode> SetupSchema(K &names, K &values, int numCols);
                static parquet::schema::NodePtr k2parquet(const std::string& name, K type);

                template<typename T, typename T1>static void writeCol(T writer, int len, T1 col);
                //static void writeCol(parquet::BoolWriter* writer, int len, B* col);
                #if KXVER>=3
                static void writeGuidCol(parquet::FixedLenByteArrayWriter* writer, K col);
                #endif
                static void writeByteCol(parquet::FixedLenByteArrayWriter* writer, K col);
                static void writeShortCol(parquet::Int32Writer* writer, K col);
                static void writeCol(parquet::Int96Writer* writer, K col);
                static void writeCharCol(parquet::ByteArrayWriter* writer, K col);
                static void writeSymCol(parquet::ByteArrayWriter* writer, K col);
                static void writeCol(parquet::ByteArrayWriter* writer, K col);
                static void writeColumn(K &col, parquet::RowGroupWriter* rg_writer);
        };
    }
}
#endif