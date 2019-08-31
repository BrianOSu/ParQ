PROG = KDBParquet
CC = g++
CPPFLAGS = -shared -fPIC -Isrc/include -lparquet -D KXVER=3
KDBFLAGS = -pthread src/l64/c.o

default: ParQ

ParQ: parquet.o writer.o reader.o
	mkdir install
	$(CC) src/lib/KDBPARQ.cpp src/lib/utils.cpp $(CPPFLAGS) $(KDBFLAGS) -o install/ParQ.so build/parquet.o build/writer.o build/reader.o

parquet.o: src/lib/parquet.cpp src/include/parquet.hpp
	mkdir -p build
	$(CC) $(CPPFLAGS) -c src/lib/parquet.cpp -o build/$@

writer.o:  src/lib/writer.cpp src/include/writer.hpp
	$(CC) $(CPPFLAGS) -c src/lib/writer.cpp -o build/$@

reader.o:  src/lib/reader.cpp src/include/reader.hpp
	$(CC) $(CPPFLAGS) -c src/lib/reader.cpp -o build/$@

install:
	mkdir -p install
	mv ParQ.so install

clean: 
	$(RM) -rf install build