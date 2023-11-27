#include "db/IntegerAggregatorHelper.h"

IntegerAggregatorHelper::IntegerAggregatorHelper() {
    this->sum = 0;
    this->min = 0;
    this->max = 0;
    this->count = 0;
    this->first = true;
}

std::vector<int> IntegerAggregatorHelper::getData() {
    return this->data;
}

int IntegerAggregatorHelper::getCount() {
    return this->count;
};

int IntegerAggregatorHelper::getMin() {
    return this->min;
}

int IntegerAggregatorHelper::getMax() {
    return this->max;
}

int IntegerAggregatorHelper::getSum() {
    return this->sum;
}

void IntegerAggregatorHelper::reset() {
    this->sum = 0;
    this->min = 0;
    this->max = 0;
    first = true;
}

void IntegerAggregatorHelper::addKey(int key) {
    if (first) {
        sum = min = max = key;
        first = false;
    } else {
        min = std::min(key, min);
        max = std::max(key, max);
        sum += key;
    }
    count++;
}
