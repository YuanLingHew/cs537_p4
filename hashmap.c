#include "hashmap.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

typedef struct {
    HashMap* hm;
    char* key;
    void* result;
} MapGetThreadArgs;

typedef struct {
    HashMap* hm;
    char* key;
    void* value;
    int value_size;
} MapPutThreadArgs;

pthread_rwlock_t rwlock;
MapGetThreadArgs* mgtargs;
MapPutThreadArgs* mptargs;
HashMap* freq;

/**
 * @brief Initializes MapGetThreaArgs
 *
 * @return MapGetThreadArgs* Pointer to HashMap
 */
MapGetThreadArgs* MapGetThreadArgsInit(HashMap* hm, char* key) {
    MapGetThreadArgs* args =
        (MapGetThreadArgs*)malloc(sizeof(MapGetThreadArgs));
    args->hm = hm;
    args->key = key;
    args->result = NULL;

    return args;
}

/**
 * @brief Initializes MapPutThreaArgs
 *
 * @return MapPutThreadArgs* Pointer to HashMap
 */
MapPutThreadArgs* MapPutThreadArgsInit(HashMap* hm, char* key, void* value,
                                       int value_size) {
    MapPutThreadArgs* args =
        (MapPutThreadArgs*)malloc(sizeof(MapPutThreadArgs));
    args->hm = hm;
    args->key = key;
    args->value = value;
    args->value_size = value_size;

    return args;
}

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

void* mapput_thread(void* args) {
    MapPutThreadArgs* arguments = (MapPutThreadArgs*)args;
    HashMap* hashmap = arguments->hm;
    char* key = arguments->key;
    void* value = arguments->value;
    int value_size = arguments->value_size;

    pthread_rwlock_wrlock(&rwlock);
    // resize hashmap if half filled
    if (hashmap->size > (hashmap->capacity / 2)) {
        if (resize_map(hashmap) < 0) {
            pthread_rwlock_unlock(&rwlock);
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
            pthread_rwlock_unlock(&rwlock);
            break;
        }
        h++;
        if (h == hashmap->capacity) h = 0;
    }

    // key not found in hashmap, h is an empty slot
    // add pair to hashmap
    hashmap->contents[h] = newpair;
    hashmap->size += 1;
    pthread_rwlock_unlock(&rwlock);
    pthread_exit(NULL);
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
    pthread_t putthread;
    mptargs = MapPutThreadArgsInit(hashmap, key, value, value_size);

    if (pthread_create(&putthread, NULL, &mapput_thread, (void*)mptargs) != 0) {
        printf("something went wrong THERE\n");
    }

    if (pthread_join(putthread, NULL) != 0) {
        printf("something went wrong HERE\n");
    }

    // free(mptargs);
}

void* mapget_thread(void* args) {
    MapGetThreadArgs* arguments = (MapGetThreadArgs*)args;
    HashMap* hashmap = arguments->hm;
    char* key = arguments->key;

    pthread_rwlock_rdlock(&rwlock);
    int h = Hash(key, hashmap->capacity);
    while (hashmap->contents[h] != NULL) {
        if (!strcmp(key, hashmap->contents[h]->key)) {
            pthread_rwlock_unlock(&rwlock);
            arguments->result = hashmap->contents[h]->value;
            break;
        }
        h++;
        if (h == hashmap->capacity) {
            h = 0;
        }
    }
    pthread_rwlock_unlock(&rwlock);
    pthread_exit(NULL);
}

/**
 * @brief Get value of key value pair
 *
 * @param hashmap Pointer to hashmap
 * @param key Char pointer to key
 * @return char* to value, NULL if not found
 */
char* MapGet(HashMap* hashmap, char* key) {
    char* result;
    pthread_t gthread;
    mgtargs = MapGetThreadArgsInit(hashmap, key);

    if (pthread_create(&gthread, NULL, &mapget_thread, (void*)mgtargs) != 0) {
        printf("something went wrong THERE\n");
    }

    if (pthread_join(gthread, NULL) != 0) {
        printf("something went wrong HERE\n");
    }

    result = (char*)mgtargs->result;
    // free(mgtargs);
    return result;
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