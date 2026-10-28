PROG = KDBParquet
CC = g++
CPPFLAGS = -fPIC -Isrc/include -D KXVER=3
CXXFLAGS = -std=c++20
LDFLAGS = -shared
LDLIBS = -lparquet
KDBFLAGS = -pthread src/l64/c.o
TEST_BIN = build/k_handles_test
PROFILE_BIN = build/parq_read_profile

default: ParQ

.PHONY: default test clean profile-reader

test: $(TEST_BIN)
	./$(TEST_BIN)

$(TEST_BIN): tests/k_handles_test.cpp src/include/kowner.hpp src/include/qforeign.hpp src/l64/c.o
	mkdir -p build
	$(CC) $(CPPFLAGS) $(CXXFLAGS) tests/k_handles_test.cpp -pthread src/l64/c.o -o $@

profile-reader: $(PROFILE_BIN)

$(PROFILE_BIN): tools/parq_read_profile.cpp src/lib/utils.cpp src/lib/parquet.cpp src/lib/reader.cpp src/lib/writer.cpp src/include/parquet.hpp src/include/reader.hpp src/include/writer.hpp src/include/utils.hpp src/include/kowner.hpp src/include/k.h src/l64/c.o
	mkdir -p build
	$(CC) tools/parq_read_profile.cpp src/lib/utils.cpp src/lib/parquet.cpp src/lib/reader.cpp src/lib/writer.cpp $(CPPFLAGS) $(CXXFLAGS) -pthread src/l64/c.o -o $@ -larrow $(LDLIBS)

ParQ: parquet.o writer.o reader.o
	mkdir -p install
	$(CC) src/lib/KDBPARQ.cpp src/lib/utils.cpp $(CPPFLAGS) $(CXXFLAGS) $(LDFLAGS) $(KDBFLAGS) -o install/ParQ.so build/parquet.o build/writer.o build/reader.o $(LDLIBS)

parquet.o: src/lib/parquet.cpp src/include/parquet.hpp
	mkdir -p build
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c src/lib/parquet.cpp -o build/$@

writer.o:  src/lib/writer.cpp src/include/writer.hpp
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c src/lib/writer.cpp -o build/$@

reader.o:  src/lib/reader.cpp src/include/reader.hpp
	$(CC) $(CPPFLAGS) $(CXXFLAGS) -c src/lib/reader.cpp -o build/$@

install:
	mkdir -p install
	mv ParQ.so install

clean: 
	$(RM) -rf install build
