#include <iostream>
#include <fstream>

#include <cstdint>
#include <sys/stat.h>
#include <limits.h>
#include <unistd.h>
#include <vector>
#include <ctime>
#include <chrono>
#include <map>

#include "utils.h"
#include "Graph.h"
#include "CycleEnumeration.h"

using namespace std;

int timeWindow = 3600;
bool temporal = true;
bool useCUnion = false;
ConcurrentCounter vertexVisits;

// *************************** function main ***********************************

int main(int argc, char **argv)
{
    if (argc == 1 || cmdOptionExists(argv, argv + argc, "-h"))
    {
        printHelp();
        return 0;
    }

    string path;
    if (cmdOptionExists(argv, argv + argc, "-f"))
        path = string(getCmdOption(argv, argv + argc, "-f"));
    else
        cout << "No input file" << endl;
    string name = path.substr(path.find_last_of("/") + 1, path.find_last_of(".") - path.find_last_of("/") - 1);

    /// Check if the file exists
    if (!pathExists(path))
    {
        cout << "The input file doesn't exist" << endl;
        return 0;
    }

    int nthr = 256;
    int algo = -1;
    if (cmdOptionExists(argv, argv + argc, "-n"))
        nthr = stol(string(getCmdOption(argv, argv + argc, "-n")));
    if (cmdOptionExists(argv, argv + argc, "-tw"))
        timeWindow = stol(string(getCmdOption(argv, argv + argc, "-tw")));
    if (cmdOptionExists(argv, argv + argc, "-tws"))
        timeWindow = stol(string(getCmdOption(argv, argv + argc, "-tws")));
    if (cmdOptionExists(argv, argv + argc, "-algo"))
        algo = stoi(string(getCmdOption(argv, argv + argc, "-algo")));
    if (cmdOptionExists(argv, argv + argc, "-cunion"))
        useCUnion = true;
    if (cmdOptionExists(argv, argv + argc, "-tw"))
        timeWindow *= 3600;

    cout << "Reading " << name << endl;
    auto read_start = chrono::steady_clock::now();
    Graph *g = new Graph;
    g->readTemporalGraph(path);
    auto read_end = chrono::steady_clock::now();
    double read_total = chrono::duration_cast<chrono::milliseconds>(read_end - read_start).count() / 1000.0;
    cout << "Graph read time time: " << read_total << " s" << endl;

    bool printResult = true;

    CycleHist resultHistogram;
    auto total_start = chrono::steady_clock::now();
    switch (algo)
    {

        // case 0:
        //     cout << " -----------------  Coarse-grained parallel JOHNSON with time window ------------------ " << endl;
        //     allCyclesJohnsonCoarseGrainedTW(g, resultHistogram, nthr);
        //     break;

        // case 1:
        //     cout << " ----------------- Fine-grained parallel JOHNSON with time window ------------------ " << endl;
        //     allCyclesJohnsonFineGrainedTW(g, resultHistogram, nthr);
        //     break;

    case 0:
        cout << " ----------------- Coarse-grained parallel READ-TARJAN with time window ------------------ " << endl;
        allCyclesReadTarjanCoarseGrainedTW(g, resultHistogram, nthr);
        break;

        // case 3:
        //     cout << " ----------------- Fine-grained parallel READ-TARJAN  with time window ------------------ " << endl;
        //     allCyclesReadTarjanFineGrainedTW(g, resultHistogram, nthr);
        //     break;

        // case 4:
        //     cout << " ----------------- Coarse-grained parallel temporal JOHNSON ----------------- " << endl;
        //     allCyclesTempJohnsonCoarseGrained(g, resultHistogram, nthr); break;

        // case 5:
        //     cout << " ----------------- Fine-grained parallel temporal JOHNSON ------------------ " << endl;
        //     allCyclesTempJohnsonFineGrained(g, resultHistogram, nthr);
        //     break;

        // case 6:
        //     cout << " ----------------- Coarse-grained parallel temporal READ-TARJAN ------------------ " << endl;
        //     allCyclesTempReadTarjanCoarseGrained(g, resultHistogram, nthr);
        //     break;

        // case 7:
        //     cout << " ----------------- Fine-grained parallel temporal READ-TARJAN ------------------ " << endl;
        //     allCyclesTempReadTarjanFineGrained(g, resultHistogram, nthr);
        //     break;

        // case 8:
        //     cout << " ----------------- Temporal Read-Tarjan ------------------ " << endl;
        //     allCyclesReadTarjanTemp(g, resultHistogram);
        //     break;

        // case 11:
        //     cout << " ------------ Cycle-union execution time ------------ " << endl;
        //     cycleUnionExecTime(g, nthr);
        //     break;

    default:
        printHelp();
        printResult = false;
        break;
    }

    auto total_end = chrono::steady_clock::now();
    double total = chrono::duration_cast<chrono::milliseconds>(total_end - total_start).count() / 1000.0;
    cout << "Total time: " << total << " s" << endl;
    cout << "Vertex visits: " << vertexVisits.getResult() << endl;
    if (printResult)
    {
        cout << "# cycle_size, num_of_cycles\n";
        unsigned long totCycles = 0;
        for (auto hist : resultHistogram)
        {
            cout << hist.first << ", " << hist.second << "\n";
            totCycles += hist.second;
        }
        cout << "Total, " << totCycles << endl;
    }
    delete g;

    return 0;
}
