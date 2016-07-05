//
// Created by vic on 16-6-28.
//




#include <memory.h>

long octelFormatCommandNumber(char *str)
{
    long num = 0;
    str++;
    while (*str != '\r'){
        num = (num*10)+(*str - '0');
        str++;
    }
    return num;
}

void octelFormatCommandSimpleString(char *str,char *ret)
{
    str++;
    size_t n = strlen(str)-2;
    memcpy(ret,str,n);
    ret[n] = '\0';
}

