#include <stdio.h>
#include <stdlib.h>

void alloc2(int** p) {
    *p = (int*)malloc(sizeof(int));
    **p = 10;
}

void alloc1(int* p) {
    // printf("%X\n", p);
    p = (int*)malloc(sizeof(int));
    // printf("%X\n", p);
    *p = 10;
}

int main() {
    int p_value = 200;
    int* p = &p_value;
    printf("Address of p_value: %X\n", &p_value);
    printf("Decimal value of p_value: %d\n", p_value);

    printf("pointer p in main() is at: %X\n", &p);
    printf("pointer p in main() points to: %X\n", p);
    printf("Decimal value of value of p: %d\n", *p);
    // alloc1(p);
    // printf("%d ", *p);  // undefined
    alloc2(&p);
    printf("%d ", *p);  // will print 10
    // free(p);
    return 0;
}