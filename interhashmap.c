#include "interhashmap.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "arraylist.h"

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

/**
 * @brief Initializes HashMap
 *
 * @return HashMap* Pointer to HashMap
 */
InterHashMap* MapInit(void) {
    InterHashMap* interhashmap = (InterHashMap*)malloc(sizeof(InterHashMap));
    interhashmap->contents =
        (ArrayList**)calloc(INTERMAP_INIT_CAPACITY, sizeof(ArrayList*));
    interhashmap->capacity = INTERMAP_INIT_CAPACITY;
    interhashmap->size = 0;
    return interhashmap;
}

/**
 * @brief Inserts key value pair in hashmap
 *
 * @param hashmap Pointer to hashmap
 * @param key Char pointer to key
 * @param value Void pointer to value
 * @param value_size int value of size of HashMap
 */
void MapPut(InterHashMap* interhashmap, char* key, void* value,
            int value_size) {
    // resize interhashmap if half filled
    if (interhashmap->size > (interhashmap->capacity / 2)) {
        if (resize_map(interhashmap) < 0) {
            exit(0);
        }
    }

    // initialize new kv pair and hash value
    MapPair* newpair = (MapPair*)malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = (void*)malloc(value_size);
    memcpy(newpair->value, value, value_size);
    h = Hash(key, interhashmap->capacity);

    // if ArrayList is not empty
    if (interhashmap->contents[h] != NULL) {
        // append
        arraylist_add(interhashmap->content[h], newpair);
    }
    // if interhashmap index is not empty
    while (interhashmap->contents[h] != NULL) {
        // if keys are equal, append to ArrayList
        if (!strcmp(key, interhashmap->contents[h]->key)) {
            free(interhashmap->contents[h]);
            interhashmap->contents[h] = newpair;
            return;
        }
        h++;
        if (h == interhashmap->capacity) h = 0;
    }

    // key not found in interhashmap, h is an empty slot
    // add pair to interhashmap
    interhashmap->contents[h] = newpair;
    interhashmap->size += 1;
}

/**
 * @brief Get value of key value pair
 *
 * @param hashmap Pointer to hashmap
 * @param key Char pointer to key
 * @return char* to value, NULL if not found
 */
char* MapGet(HashMap* hashmap, char* key) {
    int h = Hash(key, hashmap->capacity);
    while (hashmap->contents[h] != NULL) {
        if (!strcmp(key, hashmap->contents[h]->key)) {
            return hashmap->contents[h]->value;
        }
        h++;
        if (h == hashmap->capacity) {
            h = 0;
        }
    }
    return NULL;
}

/**
 * @brief Get size of hashmap
 *
 * @param map Pointer to HashMap
 * @return size_t of map size
 */
size_t MapSize(HashMap* map) { return map->size; }

/**
 * @brief Resize hashmap (double the size)
 *
 * @param map Pointer to HashMap
 * @return int 0 for success
 */
int resize_map(HashMap* map) {
    MapPair** temp;
    size_t newcapacity = map->capacity * 2;  // double the capacity

    // allocate a new hashmap table
    temp = (MapPair**)calloc(newcapacity, sizeof(MapPair*));
    if (temp == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        return -1;
    }

    size_t i;
    int h;
    MapPair* entry;
    // rehash all the old entries to fit the new table
    for (i = 0; i < map->capacity; i++) {
        if (map->contents[i] != NULL)
            entry = map->contents[i];
        else
            continue;
        h = Hash(entry->key, newcapacity);
        while (temp[h] != NULL) {
            h++;
            if (h == newcapacity) h = 0;
        }
        temp[h] = entry;
    }

    // free the old table
    free(map->contents);
    // update contents with the new table, increase hashmap capacity
    map->contents = temp;
    map->capacity = newcapacity;
    return 0;
}

/**
 * @brief FNV-1a hashing algorithm
 * https://en.wikipedia.org/wiki/Fowler-Noll-Vo_hash_function#FNV-1a_hash
 *
 * @param key char* of key
 * @param capacity size_t size of HashMap
 * @return hashed value (index of HashMap)
 */
size_t Hash(char* key, size_t capacity) {
    size_t hash = FNV_OFFSET;
    for (const char* p = key; *p; p++) {
        hash ^= (size_t)(unsigned char)(*p);
        hash *= FNV_PRIME;
        hash ^= (size_t)(*p);
    }
    return (hash % capacity);
}