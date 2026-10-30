#include <reader.hpp>

#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

void require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}

void writeNullableIntFile(const std::string& path) {
    auto output = arrow::io::FileOutputStream::Open(path).ValueOrDie();
    parquet::schema::NodeVector fields{
        parquet::schema::PrimitiveNode::Make(
            "value", parquet::Repetition::OPTIONAL, parquet::LogicalType::None(), parquet::Type::INT32)
    };
    auto schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
    auto writer = parquet::ParquetFileWriter::Open(output, schema);
    auto* rowGroup = writer->AppendRowGroup();
    auto* column = static_cast<parquet::Int32Writer*>(rowGroup->NextColumn());
    const std::array<int16_t, 2> definitionLevels{1, 0};
    const std::array<int32_t, 1> values{42};
    column->WriteBatch(2, definitionLevels.data(), nullptr, values.data());
    rowGroup->Close();
    writer->Close();
    require(output->Close().ok(), "failed to close nullable-column parquet fixture");
}

void testNullableParquetColumnIsRejected() {
    const std::string path =
        (std::filesystem::temp_directory_path() /
         ("parq-nullable-column-test-" +
          std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
          ".parquet")).string();
    try {
        writeNullableIntFile(path);
        auto fileReader = KDB::PARQ::PREADER::open_reader(path);
        auto rowGroup = fileReader->RowGroup(0);
        bool rejectedNull = false;
        try {
            K decoded = KDB::PARQ::PREADER::readColumns(rowGroup->Column(0), 2);
            if (decoded) r0(decoded);
        } catch (const std::runtime_error& error) {
            rejectedNull = std::string(error.what()) ==
                "Nullable or malformed Parquet column is unsupported";
        }
        require(rejectedNull, "nullable Parquet values should be rejected with the documented error");
        fileReader->Close();
    } catch (...) {
        std::filesystem::remove(path);
        throw;
    }
    std::filesystem::remove(path);
}

} // namespace

int main() {
    try {
        testNullableParquetColumnIsRejected();
        std::cout << "ParQ nullable-column test passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "ParQ nullable-column test failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
