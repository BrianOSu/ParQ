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

#include <parquet.hpp>

#include <climits>
#include <exception>
#include <functional>
#include <string>
#include <utility>

using namespace KDB::PARQ;

namespace {

template <typename Function>
K qBoundary(Function&& function) noexcept {
    try {
        return std::forward<Function>(function)();
    } catch (const std::exception& error) {
        return kerror(error.what());
    } catch (...) {
        return kerror("Unknown C++ exception in ParQ");
    }
}

bool isFilename(K value) noexcept {
    return value && (value->t == KC || value->t == -KC || value->t == -KS);
}

std::string filenameFrom(K value) {
    if (value->t == -KS && !value->s) {
        throw std::invalid_argument("File name symbol is null");
    }
    auto filename = k2string(value);
    if (filename.find('\0') != std::string::npos) {
        throw std::invalid_argument("File name contains a null byte");
    }
    return filename;
}

bool isColumnSelection(K value) noexcept {
    if (!value) return false;
    if (value->t == 101 || (value->t == 0 && value->n == 0)) return true;
    if (value->t != KS) return false;
    for (J i = 0; i < value->n; ++i) {
        if (!kS(value)[i]) return false;
    }
    return true;
}

bool isTextList(K value) noexcept {
    if (!value) return false;
    if (value->t == KS) {
        for (J i = 0; i < value->n; ++i) {
            if (!kS(value)[i]) return false;
        }
        return true;
    }
    if (value->t != 0) return false;
    for (J i = 0; i < value->n; ++i) {
        K item = kK(value)[i];
        if (!item || (item->t != KC && item->t != -KC && item->t != -KS)) {
            return false;
        }
        if (item->t == -KS && !item->s) return false;
    }
    return true;
}

void validateTable(K table) {
    if (!table || table->t != XT || !table->k || table->k->t != XD) {
        throw std::invalid_argument("Table argument must be a q table");
    }

    K names = kK(table->k)[0];
    K columns = kK(table->k)[1];
    if (!names || names->t != KS || !columns || columns->t != 0 || names->n != columns->n) {
        throw std::invalid_argument("Table column names and values are malformed");
    }
    for (J i = 0; i < names->n; ++i) {
        if (!kS(names)[i]) throw std::invalid_argument("Table contains a null column name");
    }

    J rowCount = 0;
    if (columns->n) {
        K firstColumn = kK(columns)[0];
        if (!firstColumn || firstColumn->t < 0) {
            throw std::invalid_argument("Table columns must be vectors");
        }
        rowCount = firstColumn->n;
    }
    for (J i = 0; i < columns->n; ++i) {
        K column = kK(columns)[i];
        if (!column || column->t < 0 || column->n != rowCount) {
            throw std::invalid_argument("Table columns must be vectors with equal row counts");
        }
    }
}

void validateMetadata(K metadata) {
    if (!metadata) throw std::invalid_argument("Metadata must be a dictionary or empty");
    if (metadata->t != XD) {
        if (metadata->t == 101 || (metadata->t == 0 && metadata->n == 0)) return;
        throw std::invalid_argument("Metadata must be a dictionary");
    }

    K keys = kK(metadata)[0];
    K values = kK(metadata)[1];
    if (!isTextList(keys) || !isTextList(values) || keys->n != values->n) {
        throw std::invalid_argument("Metadata keys and values must be matching symbol or string lists");
    }
}

} // namespace

extern "C" {
    K readGroup(K filename, K group, K cols) {
        return qBoundary([&]() -> K {
            if (!isFilename(filename)) return kerror("File name must be a string or symbol");
            if (!group || group->t != -KJ) return kerror("Group must be a long atom");
            if (group->j < 0 || group->j > INT_MAX) return kerror("Group index is out of range");
            if (!isColumnSelection(cols)) return kerror("Cols must be a symbol list or empty");
            return PKDB::readGroup(filenameFrom(filename), static_cast<int>(group->j), cols);
        });
    }

    K initReader(K filename) {
        return qBoundary([&]() -> K {
            if (!isFilename(filename)) return kerror("File name must be a string or symbol");
            return PKDB::loadReader(filenameFrom(filename));
        });
    }

    K readMulti(K cols) {
        return qBoundary([&]() -> K {
            if (!isColumnSelection(cols)) return kerror("Cols must be a symbol list or empty");
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            return instance->readCurrentGroup(cols);
        });
    }

    K nextRowGroup(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            if (!instance->advanceRowGroup()) return kerror("Already at the latest row group");
            return kb(1);
        });
    }

    K readSchema(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            return string2k(instance->schema());
        });
    }

    K readKeyValueMetadata(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");

            KOwner key{ktn(KS, 0)};
            KOwner value{ktn(KS, 0)};
            if (!key.get() || !value.get()) return kerror("Failed to allocate q metadata values");
            auto metadata = instance->keyValueMetadata();
            if (metadata) {
                for (const auto& item : metadata->sorted_pairs()) {
                    js(key.address(), ss(const_cast<char*>(item.first.c_str())));
                    js(value.address(), ss(const_cast<char*>(item.second.c_str())));
                }
            }
            return xD(key.release(), value.release());
        });
    }

    K closeP(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            return PKDB::close();
        });
    }

    K currentRowGroup(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            return kj(instance->currentRowGroup());
        });
    }

    K totalRowGroup(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PKDB::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            return kj(instance->totalRowGroups());
        });
    }

    K writer(K table, K filename, K single, K codec, K metadata) {
        return qBoundary([&]() -> K {
            if (!isFilename(filename)) return kerror("File name must be a string or symbol");
            if (!single || single->t != -KB) return kerror("Single must be a bool atom");
            if (!codec || codec->t != -KJ) return kerror("Codec must be a long atom");
            if (codec->j < 0 || codec->j > 9) return kerror("Unsupported compression codec");
            validateTable(table);
            validateMetadata(metadata);
            return PWRITE::write(table, filenameFrom(filename), single->g,
                                 parquet::Compression::type(codec->j), false, metadata);
        });
    }

    K closeW(K /*x*/) {
        return qBoundary([&]() -> K {
            auto instance = PWRITE::getInstance();
            if (!instance) return kerror("Parquet file not loaded");
            return PWRITE::close();
        });
    }
}
