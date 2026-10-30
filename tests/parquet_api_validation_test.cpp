#include <kowner.hpp>

#include <climits>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

extern "C" {
K readGroup(K filename, K group, K cols);
K initReader(K filename);
K readMulti(K handle, K cols);
K nextRowGroup(K handle);
K readSchema(K handle);
K readKeyValueMetadata(K handle);
K closeP(K handle);
K writer(K table, K filename, K single, K codec, K metadata);
K closeW(K unused);
}

namespace {

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

void expectError(K result, const std::string& expected) {
    // The standalone q C object has no q runtime to construct krr error values.
    if (!result) return;
    KOwner owner{result};
    require(owner.get() != nullptr, "API should return a q error value");
    require(owner.get()->t == -128, "API result should be a q error");
    const std::string detail = owner.get()->s ? owner.get()->s : "";
    require(detail.find(expected) != std::string::npos,
            "q error should contain '" + expected + "', got '" + detail + "'");
}

K makeMalformedTable() {
    KOwner names{ktn(KS, 2)};
    KOwner columns{ktn(0, 2)};
    require(names.get() && columns.get(), "q allocation failed while making malformed table");
    kS(names.get())[0] = ss(const_cast<char*>("short"));
    kS(names.get())[1] = ss(const_cast<char*>("long"));
    kK(columns.get())[0] = ktn(KI, 1);
    kK(columns.get())[1] = ktn(KI, 2);
    require(kK(columns.get())[0] && kK(columns.get())[1],
            "q allocation failed while making malformed table columns");
    KOwner dictionary{xD(names.release(), columns.release())};
    require(dictionary.get() != nullptr, "q allocation failed while making malformed table dictionary");
    KOwner table{xT(dictionary.release())};
    require(table.get() != nullptr, "q allocation failed while making malformed table");
    return table.release();
}

K makeValidTable() {
    KOwner names{ktn(KS, 1)};
    KOwner columns{ktn(0, 1)};
    require(names.get() && columns.get(), "q allocation failed while making test table");
    kS(names.get())[0] = ss(const_cast<char*>("value"));
    kK(columns.get())[0] = ktn(KI, 1);
    require(kK(columns.get())[0] != nullptr, "q allocation failed while making test column");
    kI(kK(columns.get())[0])[0] = 1;
    KOwner dictionary{xD(names.release(), columns.release())};
    require(dictionary.get() != nullptr, "q allocation failed while making test table dictionary");
    KOwner table{xT(dictionary.release())};
    require(table.get() != nullptr, "q allocation failed while making test table");
    return table.release();
}

void testReaderArgumentValidation() {
    KOwner group{kj(0)};
    KOwner emptyColumns{ktn(0, 0)};
    KOwner filename{kpn(const_cast<char*>("unused.parquet"), 13)};
    KOwner wrongGroup{ki(0)};
    KOwner negativeGroup{kj(-1)};
    KOwner excessiveGroup{kj(static_cast<J>(INT_MAX) + 1)};
    KOwner wrongColumns{ktn(KI, 1)};
    KOwner nullSymbolColumns{ktn(KS, 1)};
    KOwner wrongFilename{ki(0)};
    require(group.get() && emptyColumns.get() && filename.get() &&
            wrongGroup.get() && negativeGroup.get() && excessiveGroup.get() &&
            wrongColumns.get() && nullSymbolColumns.get() && wrongFilename.get(),
            "q allocation failed while preparing reader arguments");

    expectError(readGroup(nullptr, group.get(), emptyColumns.get()), "File name");
    expectError(readGroup(wrongFilename.get(), group.get(), emptyColumns.get()), "File name");
    expectError(readGroup(filename.get(), wrongGroup.get(), emptyColumns.get()), "Group must be a long atom");
    expectError(readGroup(filename.get(), negativeGroup.get(), emptyColumns.get()), "Group index is out of range");
    expectError(readGroup(filename.get(), excessiveGroup.get(), emptyColumns.get()),
                "Group index is out of range");
    expectError(readGroup(filename.get(), group.get(), wrongColumns.get()),
                "Cols must be a symbol list or empty");
    expectError(readGroup(filename.get(), group.get(), nullSymbolColumns.get()),
                "Cols must be a symbol list or empty");
    expectError(initReader(nullptr), "File name");

    KOwner nullByteFilename{kpn(const_cast<char*>("bad\0path"), 8)};
    require(nullByteFilename.get() != nullptr, "q allocation failed while preparing a null-byte filename");
    expectError(initReader(nullByteFilename.get()), "null byte");
}

void testReaderHandleValidation() {
    KOwner emptyColumns{ktn(0, 0)};
    KOwner wrongColumns{ktn(KI, 1)};
    require(emptyColumns.get() && wrongColumns.get(), "q allocation failed while preparing reader selections");

    expectError(readMulti(nullptr, wrongColumns.get()), "Cols must be a symbol list or empty");
    expectError(readMulti(nullptr, emptyColumns.get()), "Invalid q foreign object");
    expectError(nextRowGroup(nullptr), "Invalid q foreign object");
    expectError(readSchema(nullptr), "Invalid q foreign object");
    expectError(readKeyValueMetadata(nullptr), "Invalid q foreign object");
    expectError(closeP(nullptr), "Invalid q foreign object");
}

void testWriterArgumentValidation() {
    KOwner filename{kpn(const_cast<char*>("unused.parquet"), 13)};
    KOwner single{kb(1)};
    KOwner codec{kj(0)};
    KOwner metadata{ktn(0, 0)};
    KOwner wrongSingle{ktn(KB, 1)};
    KOwner wrongCodec{ki(0)};
    KOwner negativeCodec{kj(-1)};
    KOwner excessiveCodec{kj(10)};
    KOwner nonDictionaryMetadata{ki(0)};
    require(filename.get() && single.get() && codec.get() && metadata.get() &&
            wrongSingle.get() && wrongCodec.get() && negativeCodec.get() &&
            excessiveCodec.get() && nonDictionaryMetadata.get(),
            "q allocation failed while preparing writer arguments");
    KOwner validTable{makeValidTable()};

    expectError(writer(nullptr, nullptr, single.get(), codec.get(), metadata.get()), "File name");
    expectError(writer(nullptr, filename.get(), wrongSingle.get(), codec.get(), metadata.get()),
                "Single must be a bool atom");
    expectError(writer(nullptr, filename.get(), single.get(), wrongCodec.get(), metadata.get()),
                "Codec must be a long atom");
    expectError(writer(nullptr, filename.get(), single.get(), negativeCodec.get(), metadata.get()),
                "Unsupported compression codec");
    expectError(writer(nullptr, filename.get(), single.get(), excessiveCodec.get(), metadata.get()),
                "Unsupported compression codec");
    expectError(writer(nullptr, filename.get(), single.get(), codec.get(), metadata.get()),
                "Table argument must be a q table");
    expectError(writer(validTable.get(), filename.get(), single.get(), codec.get(), nonDictionaryMetadata.get()),
                "Metadata must be a dictionary");

    KOwner malformedTable{makeMalformedTable()};
    expectError(writer(malformedTable.get(), filename.get(), single.get(), codec.get(), metadata.get()),
                "equal row counts");

    KOwner malformedKeys{ktn(KI, 1)};
    KOwner metadataValues{ktn(KS, 1)};
    require(malformedKeys.get() && metadataValues.get(), "q allocation failed while preparing metadata");
    kS(metadataValues.get())[0] = ss(const_cast<char*>("value"));
    KOwner malformedMetadata{xD(malformedKeys.release(), metadataValues.release())};
    require(malformedMetadata.get() != nullptr, "q allocation failed while creating malformed metadata");
    expectError(writer(validTable.get(), filename.get(), single.get(), codec.get(), malformedMetadata.get()),
                "Metadata keys and values");
}

void testCloseWriterWithoutOpenWriter() {
    expectError(closeW(nullptr), "Parquet file not loaded");
}

} // namespace

int main() {
    try {
        testReaderArgumentValidation();
        testReaderHandleValidation();
        testWriterArgumentValidation();
        testCloseWriterWithoutOpenWriter();
        std::cout << "ParQ API validation tests passed\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "ParQ API validation tests failed: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
