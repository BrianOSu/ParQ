PROG = KDBParquet
CC = g++
CPPFLAGS = -shared -fPIC -Iinclude -lparquet
$(PROG) : 
	$(CC) lib/KDBPARQ.cpp lib/parquet.cpp lib/reader.cpp lib/writer.cpp lib/utils.cpp $(CPPFLAGS) -o ParQ.so -pthread l64/c.o -D KXVER=3
