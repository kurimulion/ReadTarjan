#include "CycleEnumeration.h"
#include <unordered_set>
#include <unordered_map>
#include <stack>
#include <utility>
#include <ctime>
#include <chrono>
#include <queue>

#ifdef MPI_IMPL
#include <mpi.h>
#endif

using namespace std;

extern int timeWindow;

bool edgeInTimeInterval(int tstart, int timeWindow, int vstart, int vert, TimestampSet &tset)
{
    auto it_start = (vstart > vert) ? lower_bound(tset.begin(), tset.end(), tstart) : upper_bound(tset.begin(), tset.end(), tstart);
    auto it_end = upper_bound(it_start, tset.end(), tstart + timeWindow);
    if (it_start >= it_end)
        return false;
    return true;
}

/// *************************** Recording cycles ***************************

void recordCycle(Cycle *current, CycleHist &result)
{
    int size = current->size();
    if (result.find(size) == result.end())
        result[size] = 0;
    result[size]++;
}

void recordBundledCycle(Cycle *current, TimestampGroups *tg, CycleHist &result)
{
    /// Check if the budle is consisted of exactly one cycle
    bool allone = true;
    long count = 1;

    for (auto it = tg->begin(); it != tg->end(); ++it)
    {
        if (it->it_end - it->it_begin > 1)
        {
            allone = false;
            break;
        }
    }

    /// Count the number of cycles in the bundle (2SCENT procedure)
    if (!allone)
    {
        queue<pair<int, long>> *prevQueue = new queue<pair<int, long>>;
        queue<pair<int, long>> *currQueue = nullptr;

        prevQueue->push(make_pair(-1, 1));

        for (auto it = tg->begin(); it != tg->end(); ++it)
        {
            currQueue = new queue<pair<int, long>>;
            int n = 0, prev = 0;
            for (auto it2 = it->it_begin; it2 != it->it_end; ++it2)
            {
                int ts = *it2;

                if (!prevQueue->empty())
                {
                    auto tmpPair = prevQueue->front();
                    while (tmpPair.first < ts)
                    {
                        prevQueue->pop();
                        n = tmpPair.second;
                        if (prevQueue->empty())
                            break;
                        tmpPair = prevQueue->front();
                    }
                }
                prev += n;
                currQueue->push(make_pair(ts, prev));
            }
            delete prevQueue;
            prevQueue = currQueue;
            currQueue = nullptr;
        }
        auto tmpPair = prevQueue->back();
        count = tmpPair.second;
        delete prevQueue;
    }

    long size = current->size();
    if (result.find(size) == result.end())
        result[size] = 0;
    result[size] += count;
}

/// *************************** Find Cycle-Unions ***********************************

const int PINF = std::numeric_limits<int>::max();

void findTempAncestors(Graph *g, EdgeData u, int firstTs, int timeWindow, BlockedMap &visited, BlockedMap *candidates = NULL)
{
    /// Temporary prevent re-visiting this vertex
    visited.insert(u.vertex, PINF);

    int mints = PINF;
    int end_tstamp = firstTs + timeWindow;

    for (int ind = g->inOffsArray[u.vertex + 1] - 1; ind >= g->inOffsArray[u.vertex]; ind--)
    {
        int w = g->inEdgeArray[ind].vertex;
        auto &tset = g->inEdgeArray[ind].tstamps;

        auto it_start = lower_bound(tset.begin(), tset.end(), u.tstamp);

        /// Update maxts
        if (it_start != tset.end())
        {
            mints = min(mints, *it_start);
        }

        /// If the timestamp is within the time interval
        if (it_start == tset.begin() || *(it_start - 1) <= firstTs)
            continue;

        int ts = *(it_start - 1);

        if (candidates && !candidates->exists(w))
            continue;

        /// Recursively visit the neighbor if it can be visited
        if (!visited.exists(w) || ts > visited.at(w))
        {
            findTempAncestors(g, EdgeData(w, ts), firstTs, timeWindow, visited, candidates);
        }
    }
    /// Update the closing time value
    visited.insert(u.vertex, mints);
}

int findCycleUnions(Graph *g, EdgeData startEdge, int startVert, int timeWindow, StrongComponent *&cunion)
{
    BlockedMap tempAncestors;
    tempAncestors.insert(startVert, PINF);

    int firstTs = startEdge.tstamp;
    for (int ind = g->inOffsArray[startVert + 1] - 1; ind >= g->inOffsArray[startVert]; ind--)
    {
        int w = g->inEdgeArray[ind].vertex;
        auto &tset = g->inEdgeArray[ind].tstamps;

        auto it_end = upper_bound(tset.begin(), tset.end(), firstTs + timeWindow);
        if (it_end == tset.begin() || *(it_end - 1) <= firstTs)
            continue;

        findTempAncestors(g, EdgeData(w, *(it_end - 1)), firstTs, timeWindow, tempAncestors); // , &tempDescendants
    }

    cunion = new StrongComponent(g->getVertexNo());
    tempAncestors.for_each([&](int el)
                           { cunion->insert(el); });

    return 0;
}

void cycleUnionExecTime(Graph *g, int numThreads)
{
    parallel_for(size_t(0), size_t(g->getVertexNo()), [&](size_t vert)
                 { parallel_for(size_t(g->offsArray[vert]), size_t(g->offsArray[vert + 1]), [&](size_t ind)
                                {
            int w = g->edgeArray[ind].vertex;
            auto &tset = g->edgeArray[ind].tstamps;

            parallel_for(size_t(0), size_t(tset.size()), [&](size_t j) {

                if ((ind + j) % size_of_cluster == process_rank) {
                    int tw = tset[j];
                    StrongComponent *cunion = NULL;
                    findCycleUnions(g, EdgeData(w, tw), vert, timeWindow, cunion);
                    delete cunion;
                }
            }); }); });
}