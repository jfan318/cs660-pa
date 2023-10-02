#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// TODO pa1.3: implement
BufferPool::BufferPool(int numPages) {
    this->numPages = numPages;
}

Page *BufferPool::getPage(const TransactionId &tid, PageId *pid) {
    // TODO pa1.3: implement
    if (buffer.count(*pid)) {
        return buffer[*pid];
    }

    // Fetch the page
    DbFile *file = Database::getCatalog().getDatabaseFile(pid->getTableId());
    Page *page = file->readPage(*pid);
    if (buffer.size() >= numPages) {
        evictPage();
    }

    // Insert the fetched page into the buffer
    buffer[*pid] = page;

    return page;
}

void BufferPool::evictPage() {
    // do nothing for now
}
