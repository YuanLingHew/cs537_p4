#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// new structs
typedef struct {
    char *key;
    char *value;
} MapPair;

typedef struct {
    MapPair **pairs;
    size_t size;
    size_t capacity;
} ArrayList;

typedef struct {
    ArrayList **contents;
    size_t capacity;
    size_t size;
} InterHashMap;

// External functions: these are what you must define
void MR_Emit(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition);

// Internal functions:
InterHashMap *MapInit(void);
ArrayList *ArrayListInit(void);
void MapPut(InterHashMap *interhashmap, char *key, char *value);
size_t Hash(char *key, size_t capacity);

#endif  // __mapreduce_h__
