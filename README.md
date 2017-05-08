# cs265
cs265 final project
Scripts....

IMPORTANT: To effectively execute scripts on your system you MUST change the first line of the s_runs,t_runs,s_current & t_current script the reflect the root directory for the source files on your machine. Also note that the absolute_path variable in the lsm2.c and threaded_lsm.c files should also be changed in the same manner.

s_runs => loads multiple data sets, in succession, into the serial version of the LSM tree

t_runs => loads multiple data sets, in succession, into the threaded version of the LSM tree

s_current => loads a single data set into the serial version of the LSM tree

t_current => loads a single data set into the threaded version of the LSM tree

Workloads:
workload_1,workload_2,workload_3,workload_4 => each represents a different combination of puts, gets, etc. per the api
