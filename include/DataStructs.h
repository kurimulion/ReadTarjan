#ifndef _DATA_STRUCTS_H_
#define _DATA_STRUCTS_H_

#include <list>
#include <cstring>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <cilk/cilk.h>

#include "Macros.h"

using namespace std;

inline void ident_cnt(void *v)
{
    *(unsigned long *)v = 0;
}

inline void add_cnt(void *l, void *r)
{
    *(unsigned long *)l += *(unsigned long *)r;
}

/// Concurrent counter for collecting statistics
// class TConcurrentCounter
// {
// public:
//     TConcurrentCounter() {}

//     void increment(unsigned long inc = 1)
//     {
//         result += inc;
//     }

//     void decrement()
//     {
//         result--;
//     }

//     unsigned long getResult()
//     {
//         return result;
//     }

// private:
//     unsigned long cilk_reducer(ident_cnt, add_cnt) result;
// };

typedef unsigned long cilk_reducer(ident_cnt, add_cnt) ConcurrentCounter;
// typedef unsigned long ConcurrentCounter;

/// Wrapper for dynamic array
template <typename T>
class VectorPath
{
public:
    VectorPath(int s) : vect(new T[s]) {}
    ~VectorPath()
    {
        if (vect)
            delete[] vect;
    }

    void push_back(T x)
    {
        ++it;
        vect[it] = x;
    }

    T back()
    {
        if (it == -1)
            return T();
        return vect[it];
    }

    void pop_back()
    {
        if (it >= 0)
            it--;
    }

    int size() { return it + 1; }

private:
    T *vect = NULL;
    int it = -1;
};

/// Wrapper for std::unordered_set
class HashSet
{
public:
    HashSet() : elems() {};
    HashSet(int s) : elems() {};
    HashSet(const HashSet &hs) : elems(hs.elems) {};

    void insert(int el) { elems.insert(el); }

    void remove(int el)
    {
        auto it = elems.find(el);
        if (it != elems.end())
            elems.erase(it);
    }

    bool exists(int el)
    {
        if (elems.find(el) != elems.end())
            return true;
        return false;
    }
    void include(const HashSet &other)
    {
        for (auto el : other.elems)
            insert(el);
    }

    int size() { return elems.size(); }
    void clear() { elems.clear(); }

    template <typename TF>
    void for_each(TF &&f)
    {
        for (auto el : elems)
            f(el);
    }
    std::unordered_set<int>::iterator begin() { return elems.begin(); }
    std::unordered_set<int>::iterator end() { return elems.end(); }
    std::unordered_set<int>::iterator erase(std::unordered_set<int>::iterator it) { return elems.erase(it); }

private:
    friend class HashSetStack;
    unordered_set<int> elems;
};

class HashSetStack
{
private:
    typedef std::mutex HashSetMutexType;
    HashSetMutexType HashSetMutex;

public:
    HashSetStack(bool conc = false) : curLevel(0), elems(), concurrent(conc), HashSetMutex() {};
    HashSetStack(int s, bool conc = false) : curLevel(0), elems(), concurrent(conc), HashSetMutex() {};
    HashSetStack(const HashSetStack &hs) : curLevel(hs.curLevel), elems(hs.elems), concurrent(hs.concurrent), HashSetMutex() {};

    HashSetStack *clone()
    {
        HashSetStack *ret;
        if (concurrent) // HashSetMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashSetMutex);
            ret = new HashSetStack(*this);
        }
        else
        {
            ret = new HashSetStack(*this);
        }
        return ret;
        // if(concurrent) HashSetMutex.unlock();
    }

    HashSetStack *clone(int lvl)
    {
        HashSetStack *ret = new HashSetStack();
        ret->curLevel = lvl;
        ret->concurrent = concurrent;

        if (concurrent) // HashSetMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashSetMutex);
            for (auto it = elems.begin(); it != elems.end(); ++it)
            {
                if (it->second <= lvl)
                    ret->elems.insert({it->first, it->second});
            }
        }
        else
        {
            for (auto it = elems.begin(); it != elems.end(); ++it)
            {
                if (it->second <= lvl)
                    ret->elems.insert({it->first, it->second});
            }
        }
        // if(concurrent) HashSetMutex.unlock();

        return ret;
    }

    void reserve(int s) {}

    void incrementLevel()
    {
        if (concurrent) // HashSetMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashSetMutex);
            curLevel++;
        }
        else
        {
            curLevel++;
        }
        // if(concurrent) HashSetMutex.unlock();
    }

    void decrementLevel()
    {
        if (concurrent) // HashSetMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashSetMutex);
            curLevel--;
            for (auto it = elems.begin(); it != elems.end();)
            {
                if (it->second > curLevel)
                    it = elems.erase(it);
                else
                    it++;
            }
        }
        else
        {
            curLevel--;
            for (auto it = elems.begin(); it != elems.end();)
            {
                if (it->second > curLevel)
                    it = elems.erase(it);
                else
                    it++;
            }
        }
        // if(concurrent) HashSetMutex.unlock();
    }

    void setLevel(int lvl)
    {
        if (concurrent) // HashSetMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashSetMutex);
            if (lvl < curLevel)
            {
                curLevel = lvl;
                for (auto it = elems.begin(); it != elems.end();)
                {
                    if (it->second > curLevel)
                        it = elems.erase(it);
                    else
                        it++;
                }
            }
        }
        else
        {
            if (lvl < curLevel)
            {
                curLevel = lvl;
                for (auto it = elems.begin(); it != elems.end();)
                {
                    if (it->second > curLevel)
                        it = elems.erase(it);
                    else
                        it++;
                }
            }
        }
        // if(concurrent) HashSetMutex.unlock();
    }

    void insert(int el)
    {
        if (concurrent) // HashSetMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashSetMutex);
            if (elems.find(el) == elems.end())
                elems[el] = curLevel;
        }
        else
        {
            if (elems.find(el) == elems.end())
                elems[el] = curLevel;
        }
        // if(concurrent) HashSetMutex.unlock();
    }

    void remove(int el)
    {
        auto it = elems.find(el);
        if (it != elems.end())
            elems.erase(it);
    }

    bool exists(int el)
    {
        if (elems.find(el) != elems.end())
            return true;
        return false;
    }

    void include(const HashSet &other)
    {
        for (auto el : other.elems)
        {
            insert(el);
        }
    }

    template <typename TF>
    void for_each(TF &&f)
    {
        for (auto el : elems)
        {
            f(el);
        }
    }

    void exclude(const HashSet &other)
    {
        for (auto el : other.elems)
        {
            remove(el);
        }
    }

    int size() { return elems.size(); }

    void clear()
    {
        elems.clear();
    }

    void copy(const HashSet &other)
    {
        elems.clear();

        for (auto el : other.elems)
        {
            insert(el);
        }
    }

    int level() { return curLevel; }

private:
    int curLevel = 0;
    unordered_map<int, int> elems;
    bool concurrent;
};

/// Wrapper for std::unordered_map
class HashMap
{
public:
    HashMap() : elems() {};
    HashMap(int s) : elems() {};
    HashMap(const HashMap &hs) : elems(hs.elems) {};

    void insert(int el, int num)
    {
        if (num != 0)
            elems[el] = num;
        else
            elems.erase(el);
    }
    void erase(int el) { elems.erase(el); }

    bool exists(int el)
    {
        auto it = elems.find(el);
        if (it != elems.end())
            return !!it->second;
        return false;
    }

    bool exists(int el, int ts)
    {
        auto it = elems.find(el);
        if (it == elems.end())
            return false;
        if (it->second == -1 || ts >= it->second)
            return true;
        return false;
    }

    int at(int el)
    {
        auto it = elems.find(el);
        if (it == elems.end())
            return 0;
        return it->second;
    }

    template <typename TF>
    void for_each(TF &&f)
    {
        for (auto el : elems)
            f(el.first);
    }
    int size() { return elems.size(); }

private:
    friend class HashMapStack;
    unordered_map<int, int> elems;
};

/// Hash map with a stack
class HashMapStack
{
private:
    struct StackElem
    {
        int cltime;
        int level;
        StackElem(int ct = 0, int l = 0) : cltime(ct), level(l) {};
    };
    typedef std::mutex HashMapMutexType;
    HashMapMutexType HashMapMutex;

public:
    HashMapStack(bool conc = false) : curLevel(0), elems(), concurrent(conc), HashMapMutex() {};
    HashMapStack(const HashMapStack &hs) : curLevel(hs.curLevel), elems(hs.elems), concurrent(hs.concurrent), HashMapMutex() {};

    HashMapStack *clone()
    {
        HashMapStack *ret;
        if (concurrent) // HashMapMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashMapMutex);
            ret = new HashMapStack(*this);
        }
        else
            ret = new HashMapStack(*this);
        // if(concurrent) HashMapMutex.unlock();
        return ret;
    }

    HashMapStack *clone(int lvl)
    {
        HashMapStack *ret = new HashMapStack();
        ret->curLevel = lvl;
        ret->concurrent = concurrent;

        if (concurrent) // HashMapMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashMapMutex);
            for (auto it = elems.begin(); it != elems.end(); ++it)
            {
                auto &vect = it->second;
                for (int ind = vect.size() - 1; ind >= 0; ind--)
                {
                    if (vect[ind].level <= lvl)
                    {
                        ret->elems.insert({it->first, vector<StackElem>(1, vect[ind])});
                        break;
                    }
                }
            }
        }
        else
        {
            for (auto it = elems.begin(); it != elems.end(); ++it)
            {
                auto &vect = it->second;
                for (int ind = vect.size() - 1; ind >= 0; ind--)
                {
                    if (vect[ind].level <= lvl)
                    {
                        ret->elems.insert({it->first, vector<StackElem>(1, vect[ind])});
                        break;
                    }
                }
            }
        }
        // if(concurrent) HashMapMutex.unlock();
        return ret;
    }

    void incrementLevel()
    {
        if (concurrent) // HashMapMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashMapMutex);
            curLevel++;
        }
        else
            curLevel++;
        // if(concurrent) HashMapMutex.unlock();
    }

    void decrementLevel()
    {
        if (concurrent) // HashMapMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashMapMutex);
            curLevel--;
            for (auto it = elems.begin(); it != elems.end();)
            {
                if (it->second.back().level > curLevel)
                    it->second.pop_back();
                if (it->second.empty())
                    it = elems.erase(it);
                else
                    ++it;
            }
        }
        else
        {
            curLevel--;
            for (auto it = elems.begin(); it != elems.end();)
            {
                if (it->second.back().level > curLevel)
                    it->second.pop_back();
                if (it->second.empty())
                    it = elems.erase(it);
                else
                    ++it;
            }
        }
        // if(concurrent) HashMapMutex.unlock();
    }

    void setLevel(int lvl)
    {
        if (concurrent) // HashMapMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashMapMutex);
            if (lvl < curLevel)
            {
                curLevel = lvl;
                for (auto it = elems.begin(); it != elems.end();)
                {
                    while (!it->second.empty() && it->second.back().level > curLevel)
                        it->second.pop_back();
                    if (it->second.empty())
                        it = elems.erase(it);
                    else
                        ++it;
                }
            }
        }
        else
        {
            if (lvl < curLevel)
            {
                curLevel = lvl;
                for (auto it = elems.begin(); it != elems.end();)
                {
                    while (!it->second.empty() && it->second.back().level > curLevel)
                        it->second.pop_back();
                    if (it->second.empty())
                        it = elems.erase(it);
                    else
                        ++it;
                }
            }
        }
        // if(concurrent) HashMapMutex.unlock();
    }

    void insert(int el, int num)
    {
        if (concurrent) // HashMapMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(HashMapMutex);
            if (num != 0)
            {
                auto it = elems.find(el);
                if (it == elems.end())
                    elems[el].push_back(StackElem(num, curLevel));
                else if (it->second.back().level < curLevel)
                    it->second.push_back(StackElem(num, curLevel));
                else if (it->second.back().level == curLevel)
                {
                    auto &last = it->second.back();
                    last.cltime = num;
                }
            }
            else
                elems.erase(el);
        }
        else
        {
            if (num != 0)
            {
                auto it = elems.find(el);
                if (it == elems.end())
                    elems[el].push_back(StackElem(num, curLevel));
                else if (it->second.back().level < curLevel)
                    it->second.push_back(StackElem(num, curLevel));
                else if (it->second.back().level == curLevel)
                {
                    auto &last = it->second.back();
                    last.cltime = num;
                }
            }
            else
                elems.erase(el);
        }
        // if(concurrent) HashMapMutex.unlock();
    }

    bool exists(int el)
    {
        auto it = elems.find(el);
        if (it != elems.end())
            return !!it->second.back().cltime;
        return false;
    }

    bool exists(int el, int ts)
    {
        auto it = elems.find(el);
        if (it == elems.end())
            return false;
        int closeTime = it->second.back().cltime;
        if (closeTime == -1 || ts >= closeTime)
            return true;
        return false;
    }

    void include(const HashMap &other)
    {
        for (auto el : other.elems)
            insert(el.first, el.second);
    }

    int at(int el)
    {
        auto it = elems.find(el);
        if (it == elems.end())
            return 0;
        return it->second.back().cltime;
    }

    int size() { return elems.size(); }

private:
    int curLevel = 0;
    unordered_map<int, vector<StackElem>> elems;
    bool concurrent;
};

/// Concurrent List
template <typename T>
class ConcurrentList
{
public:
    ConcurrentList(bool conc = false) : elems(), concurrent(conc), CListMutex() {}
    ConcurrentList(const ConcurrentList &cl) : elems(cl.elems), concurrent(cl.concurrent), CListMutex() {}
    ConcurrentList(const ConcurrentList &cl, int len) : elems(cl.elems.begin(), cl.elems.begin() + len), concurrent(cl.concurrent), CListMutex() {}

    ConcurrentList *clone()
    {
        ConcurrentList *ret = new ConcurrentList(*this);
        if (concurrent) // CListMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(CListMutex);
            ret = new ConcurrentList(*this);
        }
        else
        {
            ret = new ConcurrentList(*this);
        }
        // if(concurrent) CListMutex.unlock();
        return ret;
    }

    ConcurrentList *clone(int len)
    {
        ConcurrentList *ret;
        if (concurrent) // CListMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(CListMutex);
            ret = new ConcurrentList(*this, len);
        }
        else
        {
            ret = new ConcurrentList(*this, len);
        }
        // if(concurrent) CListMutex.unlock();
        return ret;
    }

    void push_back(T x)
    {
        if (concurrent) // CListMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(CListMutex);
            elems.push_back(x);
        }
        else
        {
            elems.push_back(x);
        }
        // if(concurrent) CListMutex.unlock();
    }

    T front() { return elems.front(); }
    T back() { return elems.back(); }

    void pop_back()
    {
        if (concurrent) // CListMutex.lock();
        {
            const std::lock_guard<std::mutex> lock(CListMutex);
            elems.pop_back();
        }
        else
        {
            elems.pop_back();
        }
        // if(concurrent) CListMutex.unlock();
    }

    void pop_back_until(int sz = 1)
    {
        while (elems.size() > sz)
        {
            if (concurrent) // CListMutex.lock();
            {
                const std::lock_guard<std::mutex> lock(CListMutex);
                elems.pop_back();
            }
            else
            {
                elems.pop_back();
            }
            // if(concurrent) CListMutex.unlock();
        }
    }

    int size() { return elems.size(); }

    template <typename TF>
    void for_each(TF &&f)
    {
        for (auto el : elems)
            f(el);
    }
    T &at(int idx) { return elems[idx]; }

    typename std::vector<T>::iterator begin() { return elems.begin(); }
    typename std::vector<T>::iterator end() { return elems.end(); }
    typename std::vector<T>::reverse_iterator rbegin() { return elems.rbegin(); }
    typename std::vector<T>::reverse_iterator rend() { return elems.rend(); }

private:
    bool concurrent = false;
    vector<T> elems;

    typedef std::mutex CListMutexType;
    CListMutexType CListMutex;
};

#endif //_DATA_STRUCTS_H_