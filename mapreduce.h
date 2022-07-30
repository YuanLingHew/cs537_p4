#ifndef __mapreduce_h__
#define __mapreduce_h__
#include "hashmap.h"
#include "stddef.h"

// Different function pointer types used by MR
typedef char *(*Getter)(char *key, int partition_number);
typedef void (*Mapper)(char *file_name);
typedef void (*Reducer)(char *key, Getter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// new structs
typedef struct {
    MapPair **pairs;
    size_t size;
    size_t capacity;
    int curr;
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
InterHashMap *InterMapInit(void);
ArrayList *ArrayListInit(void);
void InterMapPut(InterHashMap *interhashmap, char *key, char *value);
int resize_intermap(InterHashMap *interhashmap);
size_t Hash(char *key, size_t capacity);
void arraylist_allocate(ArrayList *l, unsigned int size);
void arraylist_add(ArrayList *l, MapPair *item);
void debug_print(InterHashMap *interhashmap);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
char *get_func(char *key, int partition_number);
char *InterMapGet(InterHashMap *interhashmap, char *key);

#endif  // __mapreduce_h__
