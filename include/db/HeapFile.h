#ifndef DB_HEAPFILE_H
#define DB_HEAPFILE_H

#include <iostream>
#include <fstream>
#include <vector>
#include <stdexcept>
#include <db/TupleDesc.h>
#include <db/Page.h>
#include <db/PageId.h>
#include <db/TransactionId.h>
#include <db/HeapPage.h>
#include <db/HeapPageId.h>

namespace db {

    class HeapFileIterator {
        // TODO pa1.5: add private members
        size_t index;
        size_t size;
        const std::vector<Tuple> &fields;

    public:
        HeapFileIterator(/* TODO pa1.5: add parameters */size_t i, size_t s, const std::vector<Tuple> &fields);

        bool operator!=(const HeapFileIterator &other) const;

        Tuple &operator*() const;

        HeapFileIterator &operator++();
    };

    /**
     * HeapFile is an implementation of a DbFile that stores a collection of tuples
     * in no particular order. Tuples are stored on pages, each of which is a fixed
     * size, and the file is simply a collection of those pages. HeapFile works
     * closely with HeapPage. The format of HeapPages is described in the HeapPage
     * constructor.
     *
     * @see db::HeapPage::HeapPage
     * @author Sam Madden
     */
    class HeapFile : public DbFile {
        // TODO pa1.5: add private members
        int id;
        const char *fname;
        std::fstream file;
        db::TupleDesc td;
        std::vector <HeapPage*> pages;

    public:

        /**
         * Constructs a heap file backed by the specified file.
         *
         * @param f the file that stores the on-disk backing store for this heap file.
         */
        HeapFile(const char *fname, const TupleDesc &td);

        /**
         * Returns an ID uniquely identifying this HeapFile. Implementation note:
         * you will need to generate this tableid somewhere ensure that each
         * HeapFile has a "unique id," and that you always return the same value for
         * a particular HeapFile. We suggest hashing the absolute file name of the
         * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
         * @return an ID uniquely identifying this HeapFile.
         */
        int getId() const override;

        /**
         * Returns the TupleDesc of the table stored in this DbFile.
         * @return TupleDesc of this DbFile.
         */
        const TupleDesc &getTupleDesc() const override;

        Page *readPage(const PageId &pid) override;

        /**
         * Returns the number of pages in this HeapFile.
         */
        int getNumPages() const;

        HeapFileIterator begin() const;

        HeapFileIterator end() const;
    };
}

#endif