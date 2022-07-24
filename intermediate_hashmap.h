#ifndef __intermediate_hashmap_h__
#define __intermediate_hashmap_h__
#include "arraylist.h"
#include "stddef.h"

#define INTERMAP_INIT_CAPACITY 11
#define ARRAYLIST_INIT_CAPACITY 5

typedef struct {
    char* key;
    void* value;
} MapPair;

typedef struct {
    MapPair** pairs;
    size_t size;
    size_t capacity;
} ArrayList;

typedef struct {
    ArrayList* contents[];
    size_t capacity;
    size_t size;
} InterHashMap;

// External Functions
HashMap* MapInit(void);
ArrayList* ArrayListInit(void);
void MapPut(HashMap* map, char* key, void* value, int value_size);
char* MapGet(HashMap* map, char* key);
size_t MapSize(HashMap* map);

// Internal Functions
int resize_map(HashMap* map);
size_t Hash(char* key, size_t capacity);

#endif  // __intermediate_hashmap_h__
