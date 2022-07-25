#include "test.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INTERMAP_INIT_CAPACITY 11
#define ARRAYLIST_INIT_CAPACITY 5

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

    // initializes contents (runs ArrayListInit)
    for (int i = 0; i < INTERMAP_INIT_CAPACITY; i++) {
        // initializes empty arraylists
        ArrayList* new = ArrayListInit();
        interhashmap->contents[i] = new;
    }
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

int main() {
    printf("hello worlds\n");
    interhashmap = MapInit();
    printf("%ld\n", interhashmap->capacity);
    free(interhashmap);
    return 0;
}