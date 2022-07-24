#include "arraylist.h"

#include "hashmap.h"

/**
 * @brief Initializes ArrayList
 *
 * @return ArrayList*
 */
ArrayList* ArrayListInit(void) {
    ArrayList* arraylist = malloc(sizeof(ArrayList));
    arraylist->size = 0;
    // Allocate the array
    arraylist->pairs = malloc(sizeof(MapPair*) * ARRAYLIST_INITIAL_CAPACITY);
    arraylist->capacity = ARRAYLIST_INITIAL_CAPACITY;
    return arraylist;
}

/**
 * Allocate sufficient array capacity for at least `size` elements.
 */
void arraylist_allocate(ArrayList* l, unsigned int size) {
    if (size > l->capacity) {
        unsigned int new_capacity = l->capacity;
        while (new_capacity < size) {
            new_capacity *= 2;
        }
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
 * Return item located at index.
 */
MapPair* arraylist_get(ArrayList* l, unsigned int index) {
    assert(index < l->size);
    return l->pairs[index];
}

/**
 * Return the number of items contained in the list.
 */
extern inline unsigned int arraylist_size(ArrayList* l) { return l->size; }

void arraylist_destroy(ArrayList* l) {
    free(l->pairs);
    free(l);
}
