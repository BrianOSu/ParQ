#include <kowner.hpp>
#include <parquet.hpp>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <vector>

extern "C" {
K writer(K table, K filename, K single, K codec, K metadata);
K closeW(K unused);
K readGroup(K filename, K group, K cols);
K initReader(K filename);
K readMulti(K handle, K cols);
K nextRowGroup(K handle);
K closeP(K handle);
K currentRowGroup(K handle);
K totalRowGroup(K handle);
}

namespace {

const std::array<const char*, 18> columnNames{
    "bool", "guid", "byte", "short", "int", "long", "real", "float", "char",
    "syms", "strings", "timestamp", "month", "date", "datetime", "timespan",
    "minute", "second"
};

void require(bool condition, const std::string& message) {
    if (!condition) throw std::runtime_error(message);
}

K checked(K value, const char* operation) {
    if (!value) throw std::runtime_error(std::string(operation) + " returned a null K value");
    if (value->t == -128) {
        const std::string detail = value->s ? value->s : "q error";
        r0(value);
        throw std::runtime_error(std::string(operation) + " failed: " + detail);
    }
    return value;
}

K makeColumn(int index, int start, int count) {
    K column = nullptr;
    switch (index) {
        case 0:
            column = ktn(KB, count);
            for (int i = 0; i < count; ++i) kG(column)[i] = static_cast<G>((start + i) % 2);
            break;
        case 1:
            column = ktn(UU, count);
            for (int i = 0; i < count; ++i)
                for (int byte = 0; byte < 16; ++byte)
                    kU(column)[i].g[byte] = static_cast<G>((start + i) * 17 + byte);
            break;
        case 2:
            column = ktn(KG, count);
            for (int i = 0; i < count; ++i) kG(column)[i] = static_cast<G>(start + i + 1);
            break;
        case 3:
            column = ktn(KH, count);
            for (int i = 0; i < count; ++i) kH(column)[i] = static_cast<H>(-20 + start + i);
            break;
        case 4:
            column = ktn(KI, count);
            for (int i = 0; i < count; ++i) kI(column)[i] = (start + i) * 11 - 7;
            break;
        case 5:
            column = ktn(KJ, count);
            for (int i = 0; i < count; ++i) kJ(column)[i] = 10000000000LL + start + i;
            break;
        case 6:
            column = ktn(KE, count);
            for (int i = 0; i < count; ++i) kE(column)[i] = static_cast<E>(start + i) / 3.0f;
            break;
        case 7:
            column = ktn(KF, count);
            for (int i = 0; i < count; ++i) kF(column)[i] = (start + i) / 7.0;
            break;
        case 8:
            column = ktn(KC, count);
            for (int i = 0; i < count; ++i) kC(column)[i] = static_cast<C>('a' + (start + i) % 26);
            break;
        case 9:
            column = ktn(KS, count);
            for (int i = 0; i < count; ++i) {
                const std::string symbol = "sym" + std::to_string(start + i);
                kS(column)[i] = ss(const_cast<char*>(symbol.c_str()));
            }
            break;
        case 10:
            column = ktn(0, count);
            for (int i = 0; i < count; ++i) {
                const std::string string = "text-" + std::to_string(start + i);
                kK(column)[i] = kpn(const_cast<char*>(string.c_str()), string.size());
            }
            break;
        case 11:
            column = ktn(KP, count);
            for (int i = 0; i < count; ++i) kJ(column)[i] = 123456789000000LL + (start + i) * 1000LL;
            break;
        case 12:
            column = ktn(KM, count);
            for (int i = 0; i < count; ++i) kI(column)[i] = start + i - 120;
            break;
        case 13:
            column = ktn(KD, count);
            for (int i = 0; i < count; ++i) kI(column)[i] = start + i - 9000;
            break;
        case 14:
            column = ktn(KZ, count);
            for (int i = 0; i < count; ++i) kF(column)[i] = 12000.0 + (start + i) / 86400.0;
            break;
        case 15:
            column = ktn(KN, count);
            for (int i = 0; i < count; ++i) kJ(column)[i] = (start + i) * 1000000LL;
            break;
        case 16:
        case 17:
            column = ktn(index == 16 ? KU : KV, count);
            for (int i = 0; i < count; ++i) kI(column)[i] = (start + i) * 13;
            break;
        default:
            throw std::logic_error("Unknown test column");
    }
    require(column != nullptr, "c.o failed to allocate a test column");
    return column;
}

K makeTable(int start, int count) {
    KOwner names{ktn(KS, columnNames.size())};
    KOwner columns{ktn(0, columnNames.size())};
    require(names.get() && columns.get(), "c.o failed to allocate a test table");
    for (int i = 0; i < static_cast<int>(columnNames.size()); ++i) {
        kS(names.get())[i] = ss(const_cast<char*>(columnNames[i]));
        kK(columns.get())[i] = makeColumn(i, start, count);
    }
    KOwner dictionary{xD(names.release(), columns.release())};
    require(dictionary.get() != nullptr, "c.o failed to create a table dictionary");
    K table = xT(dictionary.release());
    require(table != nullptr, "c.o failed to create a table");
    return table;
}

K columnAt(K table, int index) {
    return kK(kK(table->k)[1])[index];
}

void requireColumnEqual(K actual, K source, int index, int start, int count) {
    K got = columnAt(actual, index);
    K expected = columnAt(source, index);
    const std::string name = columnNames[index];

    if (index == 8) { // q test.q converts each one-byte Parquet string back to char.
        require(got->t == 0 && got->n == count, name + " should read as a list of strings");
        for (int i = 0; i < count; ++i) {
            K item = kK(got)[i];
            require(item && item->t == KC && item->n == 1 && kC(item)[0] == kC(expected)[i],
                    name + " value mismatch");
        }
        return;
    }
    if (index == 10) {
        require(got->t == 0 && got->n == count, name + " should read as a list of strings");
        for (int i = 0; i < count; ++i) {
            const std::string wanted = "text-" + std::to_string(start + i);
            K item = kK(got)[i];
            require(item && item->t == KC && item->n == static_cast<J>(wanted.size()) &&
                    std::string(kC(item), kC(item) + item->n) == wanted, name + " value mismatch");
        }
        return;
    }

    int expectedType = KI;
    switch (index) {
        case 0: expectedType = KB; break;
        case 1: expectedType = UU; break;
        case 2: expectedType = KG; break;
        case 3: expectedType = KH; break;
        case 4: expectedType = KI; break;
        case 5: expectedType = KJ; break;
        case 6: expectedType = KE; break;
        case 7: expectedType = KF; break;
        case 9: expectedType = KS; break;
        case 11: expectedType = KP; break;
        case 13: expectedType = KD; break;
        case 14: expectedType = KF; break; // q test.q casts the decoded float back to datetime.
        case 15: expectedType = KN; break;
        default: break;
    }
    require(got->t == expectedType && got->n == count,
            name + " returned q type " + std::to_string(got->t) +
                " instead of " + std::to_string(expectedType) +
                " or an unexpected row count");

    switch (got->t) {
        case KB: case KG:
            for (int i = 0; i < count; ++i) require(kG(got)[i] == kG(expected)[i], name + " value mismatch");
            break;
        case UU:
            for (int i = 0; i < count * 16; ++i) require(kG(got)[i] == kG(expected)[i], name + " value mismatch");
            break;
        case KH:
            for (int i = 0; i < count; ++i) require(kH(got)[i] == kH(expected)[i], name + " value mismatch");
            break;
        case KI: case KD:
            for (int i = 0; i < count; ++i) {
                const I want = index == 12 ? kI(expected)[i] :
                    (index == 16 || index == 17 ? (start + i) * 13 : kI(expected)[i]);
                require(kI(got)[i] == want, name + " value mismatch");
            }
            break;
        case KJ: case KP: case KN:
            for (int i = 0; i < count; ++i) require(kJ(got)[i] == kJ(expected)[i], name + " value mismatch");
            break;
        case KE:
            for (int i = 0; i < count; ++i) require(kE(got)[i] == kE(expected)[i], name + " value mismatch");
            break;
        case KF: case KZ:
            for (int i = 0; i < count; ++i) require(kF(got)[i] == kF(expected)[i], name + " value mismatch");
            break;
        case KS:
            for (int i = 0; i < count; ++i)
                require(std::string(kS(got)[i]) == std::string(kS(expected)[i]), name + " value mismatch");
            break;
        default:
            throw std::runtime_error(name + " has an unexpected output type");
    }
}

void requireTableRoundTrip(K actual, K source, int start, int count) {
    require(actual->t == XT && kK(kK(actual->k)[1])[0]->n == count,
            "read result should be a table with expected rows");
    K names = kK(actual->k)[0];
    require(names->n == static_cast<J>(columnNames.size()), "read table should contain every column");
    for (int i = 0; i < static_cast<int>(columnNames.size()); ++i) {
        require(std::string(kS(names)[i]) == columnNames[i], "read table column name mismatch");
        requireColumnEqual(actual, source, i, start, count);
    }
}

std::string pathFor(const char* name) {
    return (std::filesystem::temp_directory_path() /
            (std::string("parq-") + std::to_string(std::rand()) + "-" + name)).string();
}

K writeTable(K table, const std::string& path, bool single) {
    KOwner filename{kpn(const_cast<char*>(path.c_str()), path.size())};
    KOwner singleValue{kb(single)};
    KOwner codec{kj(0)}; // UNCOMPRESSED
    KOwner metadata{ktn(0, 0)};
    require(filename.get() && singleValue.get() && codec.get() && metadata.get(), "c.o allocation failed");
    return checked(writer(table, filename.get(), singleValue.get(), codec.get(), metadata.get()), "writer");
}

class ReaderHandle {
public:
    explicit ReaderHandle(const std::string& path) {
        KOwner filename{kpn(const_cast<char*>(path.c_str()), path.size())};
        require(filename.get() != nullptr, "c.o failed to allocate file path");
        handle_ = checked(initReader(filename.get()), "initReader");
    }
    ~ReaderHandle() {
        if (handle_) {
            K result = closeP(handle_);
            if (result) r0(result);
            r0(handle_);
        }
    }
    K get() const { return handle_; }
    void close() {
        K rawHandle = handle_;
        KOwner result{checked(closeP(rawHandle), "closeP")};
        handle_ = nullptr;
        r0(result.release());
        r0(rawHandle);
    }
private:
    K handle_ = nullptr;
};

void testMultiRowGroups() {
    constexpr int rowsPerGroup = 6;
    KOwner first{makeTable(0, rowsPerGroup)};
    KOwner second{makeTable(rowsPerGroup, rowsPerGroup)};
    const std::string path = pathFor("multi.parquet");
    try {
        KOwner firstWrite{writeTable(first.get(), path, false)};
        KOwner secondWrite{writeTable(second.get(), path, false)};
        KOwner closed{checked(closeW(nullptr), "closeW")};

        KOwner secondGroupNumber{kj(1)};
        KOwner selectedColumns{ktn(KS, 2)};
        require(secondGroupNumber.get() && selectedColumns.get(), "c.o allocation failed");
        kS(selectedColumns.get())[0] = ss(const_cast<char*>("int"));
        kS(selectedColumns.get())[1] = ss(const_cast<char*>("bool"));
        KOwner selectedPath{kpn(const_cast<char*>(path.c_str()), path.size())};
        KOwner selected{checked(readGroup(selectedPath.get(), secondGroupNumber.get(), selectedColumns.get()),
                                "readGroup")};
        require(selected.get()->t == XT && kK(kK(selected.get()->k)[1])[0]->n == rowsPerGroup,
                "selected group row count mismatch");
        require(std::string(kS(kK(selected.get()->k)[0])[0]) == "int" &&
                std::string(kS(kK(selected.get()->k)[0])[1]) == "bool", "selected group columns mismatch");
        require(kI(columnAt(selected.get(), 0))[0] == rowsPerGroup * 11 - 7,
                "selected int column value mismatch");

        ReaderHandle reader(path);
        KOwner groupCount{checked(totalRowGroup(reader.get()), "totalRowGroup")};
        require(groupCount.get()->j == 2, "multi-row-group file should contain two groups");
        KOwner rowGroup{checked(currentRowGroup(reader.get()), "currentRowGroup")};
        require(rowGroup.get()->j == 0, "reader should start at row group zero");
        KOwner allColumns{ktn(0, 0)};
        KOwner actualFirst{checked(readMulti(reader.get(), allColumns.get()), "readMulti first group")};
        requireTableRoundTrip(actualFirst.get(), first.get(), 0, rowsPerGroup);
        KOwner advanced{checked(nextRowGroup(reader.get()), "nextRowGroup")};
        KOwner actualSecond{checked(readMulti(reader.get(), allColumns.get()), "readMulti second group")};
        requireTableRoundTrip(actualSecond.get(), second.get(), rowsPerGroup, rowsPerGroup);
        reader.close();
    } catch (...) {
        std::filesystem::remove(path);
        throw;
    }
    std::filesystem::remove(path);
}

void testSingleRowGroup() {
    constexpr int rows = 12;
    KOwner source{makeTable(0, rows)};
    const std::string path = pathFor("single.parquet");
    try {
        KOwner wrote{writeTable(source.get(), path, true)};
        KOwner pathValue{kpn(const_cast<char*>(path.c_str()), path.size())};
        KOwner group{kj(0)};
        KOwner allColumns{ktn(0, 0)};
        KOwner actual{checked(readGroup(pathValue.get(), group.get(), allColumns.get()), "readGroup single")};
        requireTableRoundTrip(actual.get(), source.get(), 0, rows);
    } catch (...) {
        std::filesystem::remove(path);
        throw;
    }
    std::filesystem::remove(path);
}

} // namespace

int main() {
    struct TestCase {
        const char* name;
        void (*run)();
    };
    const TestCase tests[] = {
        {"Multiple row groups, selected columns, and reader advancement", testMultiRowGroups},
        {"Single row group with all supported example column types", testSingleRowGroup},
    };

    std::cout << "Running " << std::size(tests) << " ParQ round-trip tests:\n";
    std::size_t passed = 0;
    for (std::size_t i = 0; i < std::size(tests); ++i) {
        std::cout << "[" << (i + 1) << "/" << std::size(tests) << "] " << tests[i].name << " ... "
                  << std::flush;
        try {
            tests[i].run();
            ++passed;
            std::cout << "PASS\n";
        } catch (const std::exception& error) {
            std::cout << "FAIL\n  " << error.what() << '\n';
        } catch (...) {
            std::cout << "FAIL\n  Unknown exception\n";
        }
    }

    const double percentage = 100.0 * static_cast<double>(passed) / std::size(tests);
    std::cout << "Result: " << passed << "/" << std::size(tests) << " tests passed ("
              << std::fixed << std::setprecision(1) << percentage << "%)\n";
    return passed == std::size(tests) ? EXIT_SUCCESS : EXIT_FAILURE;
}
