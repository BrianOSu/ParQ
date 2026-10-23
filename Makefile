PROG = KDBParquet
CC = g++
CPPFLAGS = -fPIC -Isrc/include -D KXVER=3
CXXFLAGS = -std=c++20
LDFLAGS = -shared
LDLIBS = -lparquet
KDBFLAGS = -pthread src/l64/c.o

default: ParQ

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
