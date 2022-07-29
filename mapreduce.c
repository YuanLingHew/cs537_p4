#include "mapreduce.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INTERMAP_INIT_CAPACITY 11
#define ARRAYLIST_INIT_CAPACITY 1
#define FNV_OFFSET 14695981039346656037UL
#define FNV_PRIME 1099511628211UL

InterHashMap* interhashmap;

/**
 * @brief Initializes HashMap
 *
 * @return InterHashMap* Pointer to HashMap
 */
InterHashMap* MapInit(void) {
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
    return arraylist;
}

/**
 * @brief Inserts key value pair in hashmap
 *
 * @param interhashmap Pointer to interhashmap
 * @param key Char pointer to key
 * @param value Void pointer to value
 * @param value_size int value of size of HashMap
 */
void MapPut(InterHashMap* interhashmap, char* key, char* value) {
    // printf("MAPPUT CALLED\n");
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
    newpair->value = strdup(value);
    h = Hash(key, interhashmap->capacity);
    // printf("%d\n", h);

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
 * @brief Resize hashmap (double the size)
 *
 * @param map Pointer to HashMap
 * @return int 0 for success
 */
int resize_map(InterHashMap* interhashmap) {
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
        h = Hash(entry->pairs[0]->key, newcapacity);

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

    return 0;
}

void debug_print(InterHashMap* interhashmap) {
    printf("********************************************\n");
    printf("InterHashMap:\n");
    printf("Address:\t\tIndex:\t\tArrayList\n");
    for (int i = 0; i < interhashmap->capacity; i++) {
        printf("%X\t\t%d", &(interhashmap->contents[i]), i);
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
                    printf("%s", interhashmap->contents[i]->pairs[j]->value);
                    printf(") ");
                }
            }
            printf("]\n");
        }
    }
    printf("********************************************\n");
}

int main() {
    interhashmap = MapInit();
    char key[] = "bruh";
    char val[] = "1";
    char key2[] = "boy";
    char val2[] = "1";
    char key3[] = "beans";
    char val3[] = "1";
    char key4[] = "bitch";
    char val4[] = "1";
    char key5[] = "booobs";
    char val5[] = "1";
    char key6[] = "banana";
    char val6[] = "1";
    char key7[] = "foam";
    char val7[] = "1";
    char key8[] = "cum";
    char val8[] = "1";
    char key9[] = "star";
    char val9[] = "1";
    char key10[] = "sheesh";
    char val10[] = "1";
    MapPut(interhashmap, &key, &val);
    MapPut(interhashmap, &key, &val);
    MapPut(interhashmap, &key, &val);
    MapPut(interhashmap, &key, &val);
    MapPut(interhashmap, &key, &val);
    MapPut(interhashmap, &key2, &val2);
    MapPut(interhashmap, &key3, &val3);
    MapPut(interhashmap, &key4, &val4);
    MapPut(interhashmap, &key5, &val5);
    MapPut(interhashmap, &key6, &val6);
    MapPut(interhashmap, &key7, &val7);
    //  MapPut(interhashmap, &key8, &val8);
    //  MapPut(interhashmap, &key9, &val9);
    //  MapPut(interhashmap, &key10, &val10);

    debug_print(interhashmap);
    printf("Size: %d\n", interhashmap->size);
    printf("Capacity: %d\n", interhashmap->capacity);
    free(interhashmap);
    return 0;
}