#include "mapreduce.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hashmap.h"

#define INTERMAP_INIT_CAPACITY 11
#define ARRAYLIST_INIT_CAPACITY 1
#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

// new structs
typedef struct {
    MapPair** pairs;
    size_t size;
    size_t capacity;
    int curr;
} ArrayList;

typedef struct {
    ArrayList** contents;
    size_t capacity;
    size_t size;
} InterHashMap;

InterHashMap* interhashmap;

/**
 * @brief Initializes HashMap
 *
 * @return InterHashMap* Pointer to HashMap
 */
InterHashMap* InterMapInit(void) {
    InterHashMap* interhashmap = (InterHashMap*)malloc(sizeof(InterHashMap));
    interhashmap->contents =
        (ArrayList**)calloc(INTERMAP_INIT_CAPACITY, sizeof(ArrayList*));
    interhashmap->capacity = INTERMAP_INIT_CAPACITY;
    interhashmap->size = 0;

    /*
    // initializes contents (runs ArrayListInit)
    for (int i = 0; i < INTERMAP_INIT_CAPACITY; i++) {
        // initializes empty arraylists
        ArrayList* new = ArrayListInit();
        interhashmap->contents[i] = new;
    }
    */
    return interhashmap;
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
    arraylist->pairs =
        (MapPair**)calloc(sizeof(MapPair*), ARRAYLIST_INIT_CAPACITY);
    arraylist->capacity = ARRAYLIST_INIT_CAPACITY;
    arraylist->curr = 0;
    return arraylist;
}

/**
 * @brief Resize hashmap (double the size)
 *
 * @param map Pointer to HashMap
 * @return int 0 for success
 */
int resize_intermap(InterHashMap* interhashmap) {
    ArrayList** temp;
    size_t newcapacity = interhashmap->capacity * 2;  // double the capacity

    // allocate a new interhashmap table
    temp = (ArrayList**)calloc(newcapacity, sizeof(ArrayList*));
    if (temp == NULL) {
        printf("Malloc error! %s\n", strerror(errno));
        return -1;
    }

    size_t i;
    int h;
    ArrayList* entry;
    // rehash all the old entries to fit the new table
    for (i = 0; i < interhashmap->capacity; i++) {
        // if ArrayList exists
        if (interhashmap->contents[i] != NULL) {
            entry = interhashmap->contents[i];
        } else
            continue;

        // hash new value with new capacity
        h = MR_DefaultHashPartition(entry->pairs[0]->key, newcapacity);

        // handle collision
        while (temp[h] != NULL) {
            h++;
            if (h == newcapacity) h = 0;
        }

        temp[h] = entry;
    }

    // free the old table
    free(interhashmap->contents);
    // update contents with the new table, increase interhashmap capacity
    interhashmap->contents = temp;
    interhashmap->capacity = newcapacity;
    return 0;
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
    // printf("MAPPUT CALLED\n");
    // resize interhashmap if half filled
    if (interhashmap->size > (interhashmap->capacity / 2)) {
        if (resize_intermap(interhashmap) < 0) {
            exit(0);
        }
    }

    // initialize new kv pair and hash value
    MapPair* newpair = (MapPair*)malloc(sizeof(MapPair));
    int h;

    newpair->key = strdup(key);
    newpair->value = strdup(value);
    h = MR_DefaultHashPartition(key, interhashmap->capacity);
    // printf("%s mapped to %d\n", newpair->key, h);

    // if ArrayList exists
    while (interhashmap->contents[h] != NULL) {
        // if keys are equal, add to ArrayList
        if (!strcmp(key, interhashmap->contents[h]->pairs[0]->key)) {
            arraylist_add(interhashmap->contents[h], newpair);
            return;
        }
        // chaining when collision occurs
        h++;
        if (h == interhashmap->capacity) h = 0;
    }

    // key not found in interhashmap, h is an empty slot
    // add pair to interhashmap
    // create new ArrayList*
    ArrayList* new = ArrayListInit();
    interhashmap->contents[h] = new;
    arraylist_add(interhashmap->contents[h], newpair);
    interhashmap->size += 1;
}

/**
 * @brief Get value of key value pair
 *
 * @param interhashmap Pointer to hashmap
 * @param key Char pointer to key
 * @return char* to value, NULL if not found
 */
char* InterMapGet(InterHashMap* interhashmap, char* key) {
    int h = MR_DefaultHashPartition(key, interhashmap->capacity);
    if (interhashmap->contents[h] == 0) {
        return NULL;
    }
    return interhashmap->contents[h]->pairs[0]->value;
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
    int h = MR_DefaultHashPartition(key, interhashmap->capacity);
    // if key does not exist
    if (interhashmap->contents[h] == 0) {
        return NULL;
    }
    // find the key
    while (interhashmap->contents[h] != NULL) {
        // key found
        if (!strcmp(key, interhashmap->contents[h]->pairs[0]->key)) {
            // if ran accross every key
            if (interhashmap->contents[h]->curr >=
                interhashmap->contents[h]->size) {
                return NULL;
            }

            // increment
            interhashmap->contents[h]->curr += 1;

            return interhashmap->contents[h]->pairs[0]->value;
        }
        h++;
        if (h == interhashmap->capacity) {
            h = 0;
        }
    }
    return NULL;
}

void MR_Emit(char* key, char* value) {
    InterMapPut(interhashmap, key, value);
    return;
}

void MR_Run(int argc, char* argv[], Mapper map, int num_mappers, Reducer reduce,
            int num_reducers, Partitioner partition) {
    // intialize interhashmap
    interhashmap = InterMapInit();
    int i;
    for (i = 1; i < argc; i++) {
        // calls MR_Emit() for each word
        (*map)(argv[i]);
    }

    // debug_print_interhashmap(interhashmap);

    for (int i = 0; i < interhashmap->capacity; i++) {
        if (interhashmap->contents[i] != 0) {
            // calls Reduce() once for every key
            // printf("Calling reduce() for index %d\n", i);
            (*reduce)(interhashmap->contents[i]->pairs[0]->key, get_func,
                      interhashmap->size);
        }
    }
}