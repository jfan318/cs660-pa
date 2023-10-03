#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

// TODO pa1.3: implement
BufferPool::BufferPool(int numPages) {
    this->numPages = numPages;
}

Page *BufferPool::getPage(const TransactionId &tid, PageId *pid) {
    // TODO pa1.3: implement
    for(const auto &page:this->buffer){
        if(page->getId() == *pid){
            return page;
        }
    }

    // Fetch the page
    DbFile *file = Database::getCatalog().getDatabaseFile(pid->getTableId());
    Page *newPage = file->readPage(*pid);
    if (buffer.size() >= numPages) {
        evictPage();
    }
    // Insert the fetched page into the buffer
    buffer.emplace_back(newPage);

    return newPage;
}

void BufferPool::evictPage() {
    // do nothing for now
}