#include <db/HeapFile.h>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/HeapPage.h>
#include <stdexcept>
#include <sys/stat.h>
#include <fcntl.h>
#include <filesystem>

namespace fs = std::filesystem;
using namespace db;
using namespace fs;

//
// HeapFile
//

// TODO pa1.5: implement
HeapFile::HeapFile(const char *fname, const TupleDesc &td) {
    this->id = hash_value(fs::absolute(fname));
    this->td = td;
    this->fname = fname;
    Database::getCatalog().addTable(this, fname);

    file.open(fname, std::ios::in);
    uint8_t data[4096];
    file.read(reinterpret_cast<char*>(data), sizeof(data));

    file.close();

    HeapPage page(HeapPageId(id, 0) , data);
    pages.push_back(&page);
}

int HeapFile::getId() const {
    // TODO pa1.5: implement
    return this->id;
}

const TupleDesc &HeapFile::getTupleDesc() const {
    // TODO pa1.5: implement
    return this->td;
}

Page *HeapFile::readPage(const PageId &pid) {
    // TODO pa1.5: implement
    return pages[pid.pageNumber()];
}

int HeapFile::getNumPages() const {
    // TODO pa1.5: implement
    return this->pages.size();
}

HeapFileIterator HeapFile::begin() const {
    // TODO pa1.5: implement
    std::vector<Tuple> myTuples;
    return HeapFileIterator{0, myTuples.size(), myTuples};
}

HeapFileIterator HeapFile::end() const {
    // TODO pa1.5: implement
    std::vector<Tuple> myTuples;
    return HeapFileIterator{myTuples.size(), myTuples.size(), myTuples};
}

//
// HeapFileIterator
//

HeapFileIterator::HeapFileIterator(/* TODO pa1.5: add parameters */ size_t i, size_t s, const std::vector<Tuple> &fields)
        : index(i), size(s), fields(fields) {
    while (index < size) {
        index++;
    }
}

bool HeapFileIterator::operator!=(const HeapFileIterator &other) const {
    // TODO pa1.5: implement
    return index != other.index;
}

Tuple &HeapFileIterator::operator*() const {
    // TODO pa1.5: implement
    return const_cast<Tuple &>(fields[index]);
}

HeapFileIterator &HeapFileIterator::operator++() {
    // TODO pa1.5: implement
    do {
        index++;
    } while (index < size);
    return *this;
}
