#include "mapreduce.h"

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hashmap.h"

// new structs
typedef struct {
    MapPair** pairs;
    size_t size;
    size_t capacity;
} ArrayList;

typedef struct {
    ArrayList** contents;
    size_t capacity;
    size_t size;
} InterHashMap;

typedef struct {
    Mapper map;
    int curr;
    int numfiles;
    char** files;
} MapThreadArgs;

InterHashMap* interhashmap;
MapThreadArgs* mapthreadargs;
sem_t sem;
pthread_mutex_t mlock;

/**
 * @brief Initializes HashMap
 *
 * @return InterHashMap* Pointer to HashMap
 */
InterHashMap* InterMapInit(int capacity) {
    InterHashMap* interhashmap = (InterHashMap*)malloc(sizeof(InterHashMap));
    interhashmap->contents = (ArrayList**)calloc(capacity, sizeof(ArrayList*));
    interhashmap->capacity = capacity;
    interhashmap->size = 0;

    return interhashmap;
}

/**
 * @brief Initializes MapThreadArgs
 *
 * @return MapThreadArgs* Pointer to MapThreadArgs
 */
MapThreadArgs* MapThreadArgsInit(Mapper map, char** files, int numfiles) {
    MapThreadArgs* mtarg = (MapThreadArgs*)malloc(sizeof(MapThreadArgs));
    mtarg->map = map;
    mtarg->curr = 0;
    mtarg->numfiles = numfiles;
    mtarg->files = files;

    return mtarg;
}

/**
 * @brief Initializes ArrayList
 *
 * @return ArrayList*
 */
ArrayList* ArrayListInit(void) {
    ArrayList* arraylist = malloc(sizeof(ArrayList));
    arraylist->size = 0;
    // Allocate the array
    arraylist->pairs = (MapPair**)calloc(sizeof(MapPair*), 1);
    arraylist->capacity = 1;
    return arraylist;
}

/**
 * Allocate sufficient array capacity for at least `size` elements.
 */
void arraylist_allocate(ArrayList* l, unsigned int size) {
    if (size > l->capacity) {
        unsigned int new_capacity = l->capacity + 1;
        l->pairs = realloc(l->pairs, sizeof(MapPair*) * new_capacity);
        l->capacity = new_capacity;
    }
}

/**
 * Add item at the end of the list.
 */
void arraylist_add(ArrayList* l, MapPair* item) {
    arraylist_allocate(l, l->size + 1);
    l->pairs[l->size++] = item;
}

/**
 * @brief Inserts key value pair in hashmap
 *
 * @param interhashmap Pointer to interhashmap
 * @param key Char pointer to key
 * @param value Void pointer to value
 * @param value_size int value of size of HashMap
 */
void InterMapPut(InterHashMap* interhashmap, char* key, char* value) {
    // initialize new kv pair and hash value
    MapPair* newpair = (MapPair*)malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = strdup(value);
    newpair->marked = 0;
    h = MR_DefaultHashPartition(key, interhashmap->capacity);
    // printf("%s mapped to %d\n", newpair->key, h);

    // if ArrayList exists
    if (interhashmap->contents[h] != NULL) {
        arraylist_add(interhashmap->contents[h], newpair);
    } else {
        // key not found in interhashmap, h is an empty slot
        // add pair to interhashmap
        // create new ArrayList*
        ArrayList* new = ArrayListInit();
        interhashmap->contents[h] = new;
        arraylist_add(interhashmap->contents[h], newpair);
    }
    interhashmap->size += 1;
}

void debug_print_interhashmap(InterHashMap* interhashmap) {
    printf("********************************************\n");
    printf("InterHashMap:\n");
    printf("Address:\t\tIndex:\t\tArrayList\n");
    for (int i = 0; i < interhashmap->capacity; i++) {
        printf("%p\t\t%d", &(interhashmap->contents[i]), i);
        if (interhashmap->contents[i] == 0) {
            printf("\t\t0\n");
        } else {
            // print ArrayList
            printf("\t\t[");
            for (int j = 0; j < interhashmap->contents[i]->capacity; j++) {
                // if NULL
                if (interhashmap->contents[i]->pairs[j] == 0) {
                    printf("0 ");
                } else {
                    printf("(");
                    printf("%s", interhashmap->contents[i]->pairs[j]->key);
                    printf(", ");
                    printf("%s",
                           (char*)(interhashmap->contents[i]->pairs[j]->value));
                    printf(") ");
                }
            }
            printf("]\n");
        }
    }
    printf("********************************************\n");
}

unsigned long MR_DefaultHashPartition(char* key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0') hash = hash * 33 + c;
    return hash % num_partitions;
}

char* get_func(char* key, int partition_number) {
    // printf("get_func key(%s, %d)\n", key, partition_number);
    // printf("getting %s from partition number %d\n", key, partition_number);
    ArrayList* partition = interhashmap->contents[partition_number];
    MapPair* el;
    if (partition != 0) {
        // iterate to find key in partition
        for (int i = 0; i < partition->size; i++) {
            el = partition->pairs[i];
            if (!strcmp(el->key, key) && el->marked == 0) {
                // mark element
                el->marked = 1;
                return el->value;
            }
        }
    }
    return NULL;
}

int cmp(const void* a, const void* b) {
    char* str1 = (*(MapPair**)a)->key;
    char* str2 = (*(MapPair**)b)->key;
    return strcmp(str1, str2);
}

void* create_map_threads(void* args) {
    for (;;) {
        char* file;
        pthread_mutex_lock(&mlock);
        if (mapthreadargs->curr >= mapthreadargs->numfiles) {
            pthread_mutex_unlock(&mlock);
            return NULL;
        }
        file = mapthreadargs->files[mapthreadargs->curr];
        mapthreadargs->curr += 1;
        pthread_mutex_unlock(&mlock);
        // printf("Map(%s)\n", file);
        (*mapthreadargs->map)(file);
    }
}

// thread
void MR_Emit(char* key, char* value) {
    // get partition number
    // int h = MR_DefaultHashPartition(key, interhashmap->capacity);

    InterMapPut(interhashmap, key, value);
    return;
}

void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition) {
    // intialize interhashmap
    interhashmap = InterMapInit(num_reducers);

    // start threads for mapping phase
    if (num_mappers > argc - 1) {
        num_mappers = argc - 1;
    }
    mapthreadargs = MapThreadArgsInit(map, argv + 1, argc - 1);
    pthread_mutex_init(&mlock, NULL);
    pthread_t mthread[num_mappers];
    for (int i = 0; i < num_mappers; i++) {
        if (pthread_create(&mthread[i], NULL, &create_map_threads, NULL) != 0) {
            printf("something went wrong\n");
        }
    }

    // wait for threads to finish
    for (int i = 0; i < num_mappers; i++) {
        if (pthread_join(mthread[i], NULL) != 0) {
            printf("something went wrong\n");
        }
    }

    pthread_mutex_destroy(&mlock);

    debug_print_interhashmap(interhashmap);

    // sort each partition
    for (int i = 0; i < interhashmap->capacity; i++) {
        // checks if partition is not empty
        if (interhashmap->contents[i] != 0) {
            qsort(interhashmap->contents[i]->pairs,
                  interhashmap->contents[i]->size, sizeof(MapPair*), cmp);
        }
    }

    // debug_print_interhashmap(interhashmap);
    // Reducing Phase
    // iterate all partitions
    for (int i = 0; i < interhashmap->capacity; i++) {
        ArrayList* partition = interhashmap->contents[i];
        if (partition != 0) {
            // calls Reduce() once for every key
            // intial Reduce() for first key
            char* curr_key = partition->pairs[0]->key;
            (*reduce)(curr_key, get_func,
                      MR_DefaultHashPartition(curr_key, num_reducers));
            for (int j = 1; j < partition->size; j++) {
                // if new key encountered in same partition
                if (strcmp(partition->pairs[j]->key, curr_key)) {
                    curr_key = partition->pairs[j]->key;
                    (*reduce)(curr_key, get_func,
                              MR_DefaultHashPartition(curr_key, num_reducers));
                }
            }
        }
    }
}