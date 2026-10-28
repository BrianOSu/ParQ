#include <parquet.hpp>
#include <kowner.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

using Clock = std::chrono::steady_clock;

struct Options {
    std::filesystem::path file;
    int iterations = 1;
    int warmups = 0;
};

struct PassResult {
    double openMs = 0;
    double readMs = 0;
    double outputReleaseMs = 0;
    double closeMs = 0;
    long long rows = 0;
    int rowGroups = 0;
};

double milliseconds(Clock::duration duration) {
    return std::chrono::duration<double, std::milli>(duration).count();
}

int positiveInteger(const std::string& value, const char* option, bool allowZero) {
    size_t parsed = 0;
    int result = 0;
    try {
        result = std::stoi(value, &parsed);
    } catch (const std::exception&) {
        throw std::invalid_argument(std::string{option} + " expects an integer");
    }
    if (parsed != value.size() || result < (allowZero ? 0 : 1)) {
        throw std::invalid_argument(std::string{option} + " is out of range");
    }
    return result;
}

Options parseOptions(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string argument{argv[i]};
        if (argument == "--help" || argument == "-h") {
            std::cout << "Usage: parq_read_profile <file.parquet> [--iterations N] [--warmup N]\n";
            std::exit(EXIT_SUCCESS);
        }
        if (argument == "--iterations" || argument == "-n" || argument == "--warmup") {
            if (++i >= argc) throw std::invalid_argument(argument + " needs a value");
            const bool warmup = argument == "--warmup";
            const int value = positiveInteger(argv[i], argument.c_str(), warmup);
            if (warmup) options.warmups = value;
            else options.iterations = value;
            continue;
        }
        if (!argument.empty() && argument[0] == '-') {
            throw std::invalid_argument("Unknown option: " + argument);
        }
        if (!options.file.empty()) throw std::invalid_argument("Only one Parquet file may be specified");
        options.file = argument;
    }
    if (options.file.empty()) throw std::invalid_argument("A Parquet file path is required");
    return options;
}

PassResult readPass(const std::filesystem::path& path, bool printGroups, int iteration) {
    PassResult result;
    auto started = Clock::now();
    auto fileReader = KDB::PARQ::PREADER::open_reader(path.string());
    if (fileReader->metadata()->num_row_groups() <= 0) {
        throw std::runtime_error("Parquet file has no row groups");
    }
    auto reader = std::make_unique<KDB::PARQ::PKDB>(std::move(fileReader));
    result.openMs = milliseconds(Clock::now() - started);
    result.rowGroups = reader->totalRowGroups();

    // An empty symbol/list selection tells ParQ to convert every column.
    KOwner allColumns{ktn(0, 0)};
    if (!allColumns.get()) throw std::runtime_error("Could not allocate the ParQ column selection");

    for (int group = 0; group < result.rowGroups; ++group) {
        const auto readStarted = Clock::now();
        KOwner table{reader->readCurrentGroup(allColumns.get())};
        const double groupMs = milliseconds(Clock::now() - readStarted);
        if (!table.get() || table.get()->t != XT || !table.get()->k || table.get()->k->t != XD) {
            throw std::runtime_error("ParQ returned an invalid table");
        }

        K columns = kK(table.get()->k)[1];
        if (!columns || columns->t != 0) throw std::runtime_error("ParQ returned invalid table columns");
        const long long rows = columns->n == 0 ? 0 : kK(columns)[0]->n;
        result.rows += rows;
        result.readMs += groupMs;

        if (printGroups) {
            std::cout << "  iteration " << iteration << ", row group " << group
                      << ": " << rows << " rows, " << columns->n << " columns, "
                      << std::fixed << std::setprecision(3) << groupMs << " ms\n";
        }

        // Keep conversion time separate from freeing the resulting K table.
        const auto releaseStarted = Clock::now();
        r0(table.release());
        result.outputReleaseMs += milliseconds(Clock::now() - releaseStarted);

        if (group + 1 < result.rowGroups && !reader->advanceRowGroup()) {
            throw std::runtime_error("ParQ stopped before the final row group");
        }
    }

    started = Clock::now();
    reader->close();
    result.closeMs = milliseconds(Clock::now() - started);
    return result;
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parseOptions(argc, argv);
        if (!std::filesystem::is_regular_file(options.file)) {
            throw std::invalid_argument("Not a regular file: " + options.file.string());
        }
        const auto fileBytes = std::filesystem::file_size(options.file);

        std::cout << "ParQ standalone C++ reader profiler\n"
                  << "File: " << options.file << " (" << fileBytes << " bytes)\n"
                  << "Warmups: " << options.warmups << ", measured iterations: "
                  << options.iterations << "\n";

        for (int i = 0; i < options.warmups; ++i) {
            std::cout << "Warmup " << i + 1 << '/' << options.warmups << "...\n";
            (void)readPass(options.file, false, i + 1);
        }

        PassResult aggregate;
        for (int i = 0; i < options.iterations; ++i) {
            std::cout << "Measured iteration " << i + 1 << '/' << options.iterations << ":\n";
            const PassResult pass = readPass(options.file, true, i + 1);
            aggregate.openMs += pass.openMs;
            aggregate.readMs += pass.readMs;
            aggregate.outputReleaseMs += pass.outputReleaseMs;
            aggregate.closeMs += pass.closeMs;
            aggregate.rows += pass.rows;
            aggregate.rowGroups += pass.rowGroups;
        }

        const double divisor = static_cast<double>(options.iterations);
        const double averageReadMs = aggregate.readMs / divisor;
        const double rowsPerSecond = aggregate.readMs > 0
            ? 1000.0 * static_cast<double>(aggregate.rows) / aggregate.readMs
            : 0.0;
        const double mibPerSecond = aggregate.readMs > 0
            ? (static_cast<double>(fileBytes) * options.iterations / (1024.0 * 1024.0)) /
                  (aggregate.readMs / 1000.0)
            : 0.0;

        std::cout << "Summary (averages per measured iteration):\n"
                  << "  row groups: " << aggregate.rowGroups / options.iterations << '\n'
                  << "  rows: " << aggregate.rows / options.iterations << '\n'
                  << std::fixed << std::setprecision(3)
                  << "  open + metadata: " << aggregate.openMs / divisor << " ms\n"
                  << "  row-group reads + K conversion: " << averageReadMs << " ms\n"
                  << "  output K release: " << aggregate.outputReleaseMs / divisor << " ms\n"
                  << "  close: " << aggregate.closeMs / divisor << " ms\n"
                  << std::setprecision(1)
                  << "  throughput: " << rowsPerSecond << " rows/s, "
                  << mibPerSecond << " MiB/s (file-size based)\n";
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "parq_read_profile: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
