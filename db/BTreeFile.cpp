#include <db/BTreeFile.h>

using namespace db;

BTreeLeafPage *BTreeFile::findLeafPage(TransactionId tid, PagesMap &dirtypages, BTreePageId *pid, Permissions perm,
                                       const Field *f) {
    // TODO pa2.2: implement
    BTreePage *page = dynamic_cast<BTreePage *>(this->getPage(tid, dirtypages, pid, perm));

    if (pid->getType() == BTreePageType::LEAF) {
        return dynamic_cast<BTreeLeafPage *>(page);
    }

    BTreeInternalPage* internalPage = dynamic_cast<BTreeInternalPage*>(page);
    BTreeEntry* lastEntry = nullptr;

    for (BTreeEntry& entry : *internalPage) {
        lastEntry = &entry;
        if (f->compare(Op::LESS_THAN_OR_EQ, entry.getKey())) {
            return this->findLeafPage(tid, dirtypages, entry.getLeftChild(), Permissions::READ_ONLY, f);
        }
    }

    return this->findLeafPage(tid, dirtypages, lastEntry->getRightChild(), Permissions::READ_ONLY, f);
}

BTreeLeafPage *BTreeFile::splitLeafPage(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *page, const Field *field) {
    // TODO pa2.3: implement
    BTreeLeafPage* rightPage = dynamic_cast<BTreeLeafPage*>(this->getEmptyPage(tid, dirtypages, BTreePageType::LEAF));
    int pageNum = page->getNumTuples();
    int splitIdx = pageNum / 2;
    if (pageNum % 2 == 1) splitIdx += 1;

    auto it = page->rbegin();
    int count = 0;
    Tuple* tuple = nullptr;

    for (int i=0; i < splitIdx && it != page->rend(); ++i, ++it) {
        *tuple = *it;
        page->deleteTuple(tuple);
        rightPage->insertTuple(tuple);
        count++;
    }

    const Field& temp_field = (tuple->getField(this->keyField));

    Field* upKey = const_cast<Field*>(&temp_field);
    BTreeInternalPage* parentPage = dynamic_cast<BTreeInternalPage*>(
            this->getParentWithEmptySlots(tid, dirtypages, page->getParentId(), upKey));
    parentPage->insertEntry(*new BTreeEntry(upKey, const_cast<BTreePageId *>(&page->getId()), const_cast<BTreePageId *>(&rightPage->getId())));
    rightPage->setParentId(&parentPage->getId());

    if (page->getRightSiblingId() != nullptr) {
        BTreeLeafPage* rightSibling = dynamic_cast<BTreeLeafPage*>(
                this->getPage(tid, dirtypages, page->getRightSiblingId(), Permissions::READ_WRITE));
        rightSibling->setLeftSiblingId(const_cast<BTreePageId *>(&rightPage->getId()));
        rightPage->setRightSiblingId((BTreePageId *) &rightSibling->getId());
    }

    page->setRightSiblingId(const_cast<BTreePageId *>(&rightPage->getId()));
    rightPage->setLeftSiblingId(const_cast<BTreePageId *>(&page->getId()));

    if (field->compare(Op::LESS_THAN_OR_EQ, upKey)) return page;
    return rightPage;
}

BTreeInternalPage *BTreeFile::splitInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                                Field *field) {
    // TODO pa2.3: implement
    BTreeInternalPage* rightPage = dynamic_cast<BTreeInternalPage*>(this->getEmptyPage(tid, dirtypages, BTreePageType::INTERNAL));
    int pageNum = page->getNumEntries();
    int splitIdx = pageNum / 2;
    if (pageNum % 2 == 0) splitIdx -= 1;

    auto it = page->rbegin();
    BTreeEntry* entry = nullptr;

    int count = 0;
    for (int i=0; i < splitIdx && it != page->rend(); ++i, ++it) {
        *entry = *it;
        page->deleteKeyAndRightChild(entry);
        rightPage->insertEntry(*entry);
        count++;
    }

    auto upEntry =  *(++it);
    page->deleteKeyAndRightChild(&upEntry);

    BTreeInternalPage* parentPage = dynamic_cast<BTreeInternalPage*>(this->getParentWithEmptySlots(tid, dirtypages, page->getParentId(), upEntry.getKey()));

    parentPage->insertEntry(upEntry);
    rightPage->setParentId(const_cast<BTreePageId *>(&parentPage->getId()));

    this->updateParentPointers(tid, dirtypages, page);
    this->updateParentPointers(tid, dirtypages, rightPage);

    if (field->compare(Op::LESS_THAN_OR_EQ, upEntry.getKey()))
        return page;
    return rightPage;
}

void BTreeFile::stealFromLeafPage(BTreeLeafPage *page, BTreeLeafPage *sibling, BTreeInternalPage *parent,
                                  BTreeEntry *entry, bool isRightSibling) {
    // TODO pa2.4: implement (BONUS)
    int moveNum = (sibling->getNumTuples() - page->getNumTuples()) / 2;
    auto it = sibling->rbegin();
    Tuple tuple;

    if (isRightSibling) {
        it = sibling->begin();
    }

    for (int i=0; i < moveNum; ++i) {
        tuple = (++it).operator*();
        sibling->deleteTuple(&tuple);
        page->insertTuple(&tuple);
    }

    if (isRightSibling) {
        tuple = (++it).operator*();
    }
    entry->setKey(const_cast<db::Field*>(&tuple.getField(keyField)));
    parent->updateEntry(entry);
}

void BTreeFile::stealFromLeftInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                          BTreeInternalPage *leftSibling, BTreeInternalPage *parent,
                                          BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
    int moveNum = (leftSibling->getNumEntries() - page->getNumEntries()) / 2;
    auto it = leftSibling->rbegin();
    auto pageIt = page->begin();
    Tuple tuple;

    BTreePageId* lChildId = (*(++it)).getRightChild();
    BTreePageId* rChildId = (*(++pageIt)).getLeftChild();

    BTreeEntry entry(parentEntry->getKey(), nullptr, rChildId);

    for (int i=0; i < moveNum; ++i) {
        BTreeEntry leftEntry = *(++it);
        entry.setLeftChild(leftEntry.getRightChild());
        entry = BTreeEntry(leftEntry.getKey(), nullptr, leftEntry.getRightChild());
        leftSibling->deleteKeyAndLeftChild(&leftEntry);
    }

    parentEntry->setKey(entry.getKey());
    parent->updateEntry(parentEntry);
    updateParentPointers(tid, dirtypages, page);
    updateParentPointers(tid, dirtypages, leftSibling);
}

void BTreeFile::stealFromRightInternalPage(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *page,
                                           BTreeInternalPage *rightSibling, BTreeInternalPage *parent,
                                           BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
    int moveNum = (rightSibling->getNumEntries() - page->getNumEntries()) / 2;
    auto it = rightSibling->rbegin();
    auto pageIt = page->begin();
    Tuple tuple;

    BTreePageId* lChildId = (*(++it)).getRightChild();
    BTreePageId* rChildId = (*(++pageIt)).getLeftChild();

    BTreeEntry entry(parentEntry->getKey(), lChildId, nullptr);

    for (int i=0; i < moveNum; ++i) {
        BTreeEntry rightEntry = *(++it);
        entry.setRightChild(rightEntry.getLeftChild());
        entry = BTreeEntry(rightEntry.getKey(), rightEntry.getLeftChild(), nullptr);
        rightSibling->deleteKeyAndRightChild(&rightEntry);
    }

    parentEntry->setKey(entry.getKey());
    parent->updateEntry(parentEntry);
    updateParentPointers(tid, dirtypages, page);
    updateParentPointers(tid, dirtypages, rightSibling);
}

void BTreeFile::mergeLeafPages(TransactionId tid, PagesMap &dirtypages, BTreeLeafPage *leftPage,
                               BTreeLeafPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}

void BTreeFile::mergeInternalPages(TransactionId tid, PagesMap &dirtypages, BTreeInternalPage *leftPage,
                                   BTreeInternalPage *rightPage, BTreeInternalPage *parent, BTreeEntry *parentEntry) {
    // TODO pa2.4: implement (BONUS)
}
