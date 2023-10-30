#include <db/BufferPool.h>
#include <db/Database.h>

using namespace db;

void BufferPool::evictPage() {
    // TODO pa2.1: implement
    // Check if there are any pages in the buffer pool
    if (pages.size() == 0) {
        throw std::runtime_error("No pages in buffer pool to evict");
    }

    // Select the first page in the buffer pool for eviction
    auto it = pages.begin();
    const PageId *pid = it->first;
    Page *page = it->second;

    // Flush the page to disk if it is dirty
    flushPage(pid);

    // Remove the page from the buffer pool
    pages.erase(it);

    delete page;
}

void BufferPool::flushAllPages() {
    // TODO pa2.1: implement
    for (auto &entry : pages) {
        flushPage(entry.first);
    }
}

void BufferPool::discardPage(const PageId *pid) {
    // TODO pa2.1: implement
    pages.erase(pid);
}

void BufferPool::flushPage(const PageId *pid) {
    // TODO pa2.1: implement
    DbFile * file = Database::getCatalog().getDatabaseFile(pid->getTableId());
    if (pages[pid]->isDirty()) {
        file->writePage(pages[pid]);
        pages[pid]->markDirty(std::nullopt);
    }
}

void BufferPool::flushPages(const TransactionId &tid) {
    // TODO pa2.1: implement
    for (auto &entry : pages) {
        if (entry.second->isDirty() == tid) {
            flushPage(entry.first);
        }
    }
}

void BufferPool::insertTuple(const TransactionId &tid, int tableId, Tuple *t) {
    // TODO pa2.3: implement
    DbFile *file = Database::getCatalog().getDatabaseFile(tableId);

    if (!file) {
        throw std::runtime_error("Not found");
    }

    // Insert the tuple into the table
    file->insertTuple(tid, *t);

    // Add the page containing the tuple to the bufferpool
    Page *page = getPage(t->getRecordId()->getPageId());
    page->markDirty(tid);
}

void BufferPool::deleteTuple(const TransactionId &tid, Tuple *t) {
    // TODO pa2.3: implement
    DbFile *file = Database::getCatalog().getDatabaseFile(t->getRecordId()->getPageId()->getTableId());

    if (!file) {
        throw std::runtime_error("Not found");
    }

    file->deleteTuple(tid, *t);

    Page *page = getPage(t->getRecordId()->getPageId());
    page->markDirty(tid);
}
