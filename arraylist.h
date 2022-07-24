#ifndef __arraylist_h__
#define __arraylist_h__
#include "intermediate_hashmap.h"
#include "stddef.h"

#define ARRAYLIST_INIT_CAPACITY 5

typedef struct {
    MapPair** pairs;
    size_t size;
    size_t capacity;
} ArrayList;

// External Functions
ArrayList* ArrayListInit(void);
void arraylist_allocate(ArrayList* l, unsigned int size);
void arraylist_add(ArrayList* l, MapPair* item);
MapPair* arraylist_get(ArrayList* l, unsigned int index);
extern inline unsigned int arraylist_size(ArrayList* l);
void arraylist_destroy(ArrayList* l);

#endif  // __arraylist_h__
