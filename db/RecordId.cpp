#include <db/RecordId.h>
#include <stdexcept>

using namespace db;

//
// RecordId
//

// TODO pa1.4: implement
RecordId::RecordId(const PageId *pid, int tupleno) {
    this->pid = pid;
    this->tupleno = tupleno;
}

bool RecordId::operator==(const RecordId &other) const {
    // TODO pa1.4: implement
    if (this->getPageId()->getTableId() == other.getPageId()->getTableId()
    && this->getPageId()->pageNumber() == this->getPageId()->pageNumber()
    && this->getTupleno() == other.getTupleno()){
        return true;
    }
    return false;
}

const PageId *RecordId::getPageId() const {
    // TODO pa1.4: implement
    return this->pid;
}

int RecordId::getTupleno() const {
    // TODO pa1.4: implement
    return this->tupleno;
}

//
// RecordId hash function
//

std::size_t std::hash<RecordId>::operator()(const RecordId &r) const {
    // TODO pa1.4: implement
    std::size_t h1 = std::hash<int>{}(r.getPageId()->getTableId() ^ r.getPageId()->pageNumber());
    std::size_t h2 = std::hash<int>{}(r.getTupleno());
    return h1 ^ h2;
}
