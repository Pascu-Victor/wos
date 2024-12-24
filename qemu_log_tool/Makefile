# Makefile for building resolve_addresses

CXX = clang++
INCLUDES = -I/usr/include -I/usr/include/tbb
CXXFLAGS = -lbfd -liberty -Wall -std=c++23 -march=native -O3 -ltbb
TARGET = resolve_addresses
SRC = resolve_addresses.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(INCLUDES) -o $(TARGET) $(SRC) $(CXXFLAGS)

clean:
	rm -f $(TARGET)
