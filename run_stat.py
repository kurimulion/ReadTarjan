import os

num_threads = [2**i for i in range(11)]
datasets = ['CollegeMsg', 'formatted_soc-sign-bitcoinalpha']
binary = ['cilk_for_cycle', 'recursive_for_cycle']

cmd = 'CILK_NWORKERs={0} ./build/{1} -f ./data/{2}.txt -algo 0 -tw 2 &> ./log/{3}'

for b in binary:
    for d in datasets:
        for n in num_threads:
            out_path = f'{d}_{b}_{n}_2hrs'
            os.system(cmd.format(n, b, d, out_path))
            exit(0)
