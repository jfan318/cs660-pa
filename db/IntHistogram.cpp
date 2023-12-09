#include <db/IntHistogram.h>
#include <vector>
#include <cmath>

using namespace db;

IntHistogram::IntHistogram(int buckets, int min, int max) {
    // TODO pa4.1: some code goes here
    this->numBuckets = buckets;
    this->buckets = std::vector<int>(buckets);

    this->min = min;
    this->max = max;

    this->bucketWidth = (int) ceil((max - min + 1) / (double) numBuckets);
    this->totalValues = 0;
}

void IntHistogram::addValue(int v) {
    // TODO pa4.1: some code goes here
    int currentBucket = (v - this->min) / this->bucketWidth;
    if (currentBucket < this->numBuckets) {
        this->buckets[currentBucket]++;
        this->totalValues++;
    }
}

double IntHistogram::estimateSelectivity(Predicate::Op op, int v) const {
    // TODO pa4.1: some code goes here
    int currentBucket = (v - this->min) / this->bucketWidth;

    // empty histogram
    if (totalValues == 0) {
        return 0;
    }

    // values outside its range of data
    if (v < this->min) {
        if (op ==Predicate::Op::LESS_THAN ||op==Predicate::Op::LESS_THAN_OR_EQ) {
            return 0;
        }
        else {
            return 1;
        }
    }

    if (v > this->max) {
        if (op ==Predicate::Op::LESS_THAN ||op==Predicate::Op::LESS_THAN_OR_EQ) {
            return 1;
        }
        else {
            return 0;
        }
    }

    double result = 0.0;
    switch(op) {
        case Predicate::Op::EQUALS:
            result = (buckets[currentBucket] / (double) bucketWidth) / totalValues;
            break;

        case Predicate::Op::GREATER_THAN:
            for (int i=v+1; i < max; i++) {
                currentBucket = (i - min) / bucketWidth;
                result += (buckets[currentBucket] / (double) bucketWidth) / totalValues;
            }
            break;

        case Predicate::Op::LESS_THAN:
            for (int i=v-1; i >= min; i--) {
                currentBucket = (i - min) / bucketWidth;
                result += (buckets[currentBucket] / (double) bucketWidth) / totalValues;
            }
            break;

        case Predicate::Op::NOT_EQUALS:
            result = 1.0 - estimateSelectivity(Predicate::Op::EQUALS, v);
            break;

        case Predicate::Op::GREATER_THAN_OR_EQ:
            result = estimateSelectivity(Predicate::Op::GREATER_THAN, v - 1);
            break;

        case Predicate::Op::LESS_THAN_OR_EQ:
            result =  estimateSelectivity(Predicate::Op::LESS_THAN, v + 1);
            break;

        case Predicate::Op::LIKE:
            break;
    }
    return result;
}

double IntHistogram::avgSelectivity() const {
    // TODO pa4.1: some code goes here
    double total = 0.0;
    for (int bucket : buckets) {
        total += (bucket / (double) bucketWidth) / totalValues;
    }
    double avg = total/numBuckets;
    return avg;
}

std::string IntHistogram::to_string() const {
    // TODO pa4.1: some code goes here
    std::string s;
    for (int i = 0; i < this->numBuckets; i++) {
        s += "bucket " + std::to_string(i) + ": ";
        for (int j = 0; j < this->buckets[i]; j++) {
            s += "|";
        }
        s += "\n";
    }
    return s;
}
