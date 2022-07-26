#include "hashmap.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

/**
 * @brief Initializes HasMap
 *
 * @return HashMap* Pointer to HashMap
 */
HashMap* MapInit(void) {
    HashMap* hashmap = (HashMap*)malloc(sizeof(HashMap));
    hashmap->contents = (MapPair**)calloc(MAP_INIT_CAPACITY, sizeof(MapPair*));
    hashmap->capacity = MAP_INIT_CAPACITY;
    hashmap->size = 0;
    return hashmap;
}

/**
 * @brief Inserts key value pair in hashmap
 *
 * @param hashmap Pointer to hashmap
 * @param key Char pointer to key
 * @param value Void pointer to value
 * @param value_size int value of size of HashMap
 */
void MapPut(HashMap* hashmap, char* key, void* value, int value_size) {
    // resize hashmap if half filled
    if (hashmap->size > (hashmap->capacity / 2)) {
        if (resize_map(hashmap) < 0) {
            exit(0);
        }
    }

    // initialize new kv pair and hash value
    MapPair* newpair = (MapPair*)malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = (void*)malloc(value_size);
    newpair->marked = 0;
    memcpy(newpair->value, value, value_size);
    h = Hash(key, hashmap->capacity);

    // if hashmap index is not empty
    while (hashmap->contents[h] != NULL) {
        // if keys are equal, update (overrides)
        if (!strcmp(key, hashmap->contents[h]->key)) {
            free(hashmap->contents[h]);
            hashmap->contents[h] = newpair;
            return;
        }
        h++;
        if (h == hashmap->capacity) h = 0;
    }

    // key not found in hashmap, h is an empty slot
    // add pair to hashmap
    hashmap->contents[h] = newpair;
    hashmap->size += 1;
}

/**
 * @brief Get value of key value pair
 *
 * @param hashmap Pointer to hashmap
 * @param key Char pointer to key
 * @return char* to value, NULL if not found
 */
void* MapGet(HashMap* hashmap, char* key) {
    int h = Hash(key, hashmap->capacity);
    while (hashmap->contents[h] != NULL) {
        if (!strcmp(key, hashmap->contents[h]->key)) {
            // printf("key: %s FOUND!\n", key);
            // printf("returning %s\n", (char*)hashmap->contents[h]->value);
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

void debug_print_hashmap(HashMap* hashmap) {
    printf("********************************************\n");
    printf("HashMap:\n");
    printf("Address:\t\tIndex:\t\tMapPair\n");
    for (int i = 0; i < hashmap->capacity; i++) {
        printf("%p\t\t%d", &(hashmap->contents[i]), i);
        if (hashmap->contents[i] == 0) {
            printf("\t\t0\n");
        } else {
            // print MapPair
            printf("\t\t(%s, %d)\n", hashmap->contents[i]->key,
                   *(int*)hashmap->contents[i]->value);
        }
    }
    printf("********************************************\n");
}