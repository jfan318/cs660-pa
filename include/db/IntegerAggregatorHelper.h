#ifndef CS660_INTEGERAGGREGATORHELPER_H
#define CS660_INTEGERAGGREGATORHELPER_H

#include <vector>

class IntegerAggregatorHelper {
public:
    int sum;
    int min;
    int max ;
    int count;
    bool first;
    std::vector<int> data;

    IntegerAggregatorHelper();

    std::vector<int> getData();

    int getMin();

    int getCount();

    int getMax();

    int getSum();

    void reset();

    void addKey(int key);
};

#endif //CS660_INTEGERAGGREGATORHELPER_H
