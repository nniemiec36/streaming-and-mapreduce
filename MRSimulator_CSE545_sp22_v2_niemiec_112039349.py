##########################################################################
## MRSimulator_CSE545_sp22.py  v2
##
## Implements a basic version of MapReduce intended to run
## on multiple threads of a single system. This implementation
## is simply intended as an instructional tool for students
## to better understand what a MapReduce system is doing
## in the backend in order to better understand how to
## program effective mappers and reducers. 
##
## MyMRSimulator is meant to be inheritted by programs
## using it. See the example "WordCountMR" class for 
## an exaample of how a map reduce programmer would
## use the MyMRSimulator system by simply defining
## a map and a reduce method. 
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
##
## Student Name: Nicole Niemiec
## Student ID: #112039349


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random
import math


##########################################################################
##########################################################################
# MapReduceSystem: 

class MyMRSimulator:
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3, use_combiner = False): 
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks=num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks=num_reduce_tasks # " " " as reduce tasks
        self.use_combiner = use_combiner #whether or not to use a combiner within map task
        
    ###########################################################   
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): 
        print("Need to override map")

    
    @abstractmethod
    def reduce(self, k, vs): 
        print("Need to overrirde reduce")
        

    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, namenode_m2r, combiner=False): 
        #runs the mappers on each record within the data_chunk and assigns each k,v to a reduce task
        mapped_kvs = [] #stored keys and values resulting from a map 
        for (k, v) in data_chunk:
            #run mappers:
            chunk_kvs = self.map(k, v) #the resulting keys and values after running the map task
            mapped_kvs.extend(chunk_kvs) 
			
	#assign each kv pair to a reducer task
        if combiner:
            for_early_reduce = dict()#holds k, vs for running reduce
            #1. Setup value lists for reducers
            for (k, v) in mapped_kvs:
                try: 
                    for_early_reduce[k].append(v)
                except KeyError:
                    for_early_reduce[k] = [v]

            #2. call reduce, appending result to get passed to reduceTasks
            for k, vs in for_early_reduce.items():
                namenode_m2r.append((self.partitionFunction(k), self.reduce(k, vs)))
            
        else:
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))


    def partitionFunction(self,k): 
        #given a key returns the reduce task to send it
        node_number = np.sum([ord(c) for c in str(k)]) % self.num_reduce_tasks
        return node_number


    def reduceTask(self, kvs, namenode_fromR): 
        #SEGMENT 1. Sort such that all values for a given key are in a 
        #           list for that key 
        #[TODO]#
        key_dict = dict()
        for (k, v) in kvs:
            # print("(k, v): ", (k, v))
            try:
                # print("\n(k, v): ", (k,v))
                key_dict[k].append(v)
                # print("key_dict[k]: ", key_dict[k])
                # print(key_dict)
            except KeyError:
                key_dict[k] = []
                key_dict[k].append(v)
        # print("key_dict: 110", key_dict)
        
        #SEGMENT 2. call self.reduce(k, vs) for each each key, providing 
        #           its list of values and append the results (if they exist) 
        #           to the list variable "namenode_fromR" 
        for (k, vs) in key_dict.items():
            if vs:
                reduced = self.reduce(k, vs)
                if reduced: #if nothing is returned, no data is there, so we skip
                    namenode_fromR.append(reduced)
        # print("namenode_fromR 119: ", namenode_fromR)
        

    def runSystem(self): 
        #runs the full map-reduce system processes on mrObject

        #[SEGMENT 1]
        #the following two lists are shared by all processes
        #in order to simulate the communication
        namenode_m2r = Manager().list() #stores the reducer task assignment and 
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
                                          #[COMBINER: when enabled this might hold]
        namenode_fromR = Manager().list() #stores key-value pairs returned from reducers
                                          #in the form [(k, v), ...]
        
        #[SEGMENT 2]
        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it. 
        #the following starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()  
        processes = []
        chunkSize = int(np.ceil(len(self.data) / int(self.num_map_tasks)))
        for i in range(0, len(self.data), chunkSize):
            chunk = self.data[i : (min(len(self.data), i+chunkSize))]
            # goes from i to the min of the length of data to chunkSize
            # it's a splice (?) unsure if that's the word
            processed = Process(target=self.mapTask, args=(chunk, namenode_m2r, self.use_combiner))
            # need to create processes
            processed.start()
            processes.append(processed)
 

        #[SEGMENT 3]
        #join map task processes back
        for p in processes:
            p.join()
		        #print output from map tasks 
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        ##[SEGMENT 4]
        #"send" each key-value pair to its assigned reducer by placing each 
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)] 
        
        for i in range(len(namenode_m2r)):
            try:
                to_reduce_task[namenode_m2r[i][0]].append(namenode_m2r[i][1])
            except:
                print("error: ", namenode_m2r[i][0], self.num_reduce_tasks)


        #[SEGMENT 5]
        #launch the reduce tasks as a new process for each. 
        processes = []
        for kvs in to_reduce_task:
            processes.append(Process(target=self.reduceTask, args=(kvs, namenode_fromR)))
            processes[-1].start()

        #[SEGMENT 6]
        #join the reduce tasks back
        for p in processes:
            p.join()
        #print output from reducer tasks 
        print("namenode_fromR after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        #return all key-value pairs:
        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:
            
class WordCountMR(MyMRSimulator): #[DONE: Example]
    #the mapper and reducer for word count
    def map(self, k, v): #[DONE]
        counts = dict()
        for w in v.split():
            w = w.lower() #makes this case-insensitive
            try:  #try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()
    
    def reduce(self, k, vs): #[DONE]
        return (k, np.sum(vs))        

class MatrixMultMR(MyMRSimulator): #[DONE:Example]
    def map(self, k, v):
        # print("k 212: ", k);
        pairs = []
        (name_dim, i, j) = k
        name, mdims, ndims = [s.split(',') for s in name_dim.split(':')]
        newname = 'AxB:'+str(mdims[0])+':'+str(ndims[1])

        #send each A to the cells it is needed for the final matrix
        if name[0] == 'A':
            for a in range(int(ndims[1])):
                pairs.append(((newname, i, a), ('m', j, v)))
        #send each B to cells it is needed for the final matrix
        elif name[0] == 'B':
            j, a = i, j#for n we are ordering differently
            for i in range(int(mdims[0])):
                pairs.append(((newname, i, a), ('n', j, v)))
        return pairs
        
    
    def reduce(self, k, vs):      
        rowcolSum = 0#stores the sum
        #separate m and n, keyed by j
        valsByJMat = dict()
        for (matrix, j, v) in vs:
            try:
                valsByJMat[j][matrix] = v
            except KeyError:
                valsByJMat[j] = {matrix: v}

        #sum product of m and n js:
        for j, vals in valsByJMat.items():
            if len(vals) > 1:
                rowcolSum += vals['m'] * vals['n']

        return (k, rowcolSum)

class CountBy10PowersMR(MyMRSimulator): 

    def map(self, k, v): 
        tens = dict()
        exp = math.log(k, 10)
        exp = math.floor(exp)
        power = 10**exp
        try:
            tens[power] += 1
        except:
            tens[power] = 1
        return tens.items()
    
    def reduce(self, k, vs): 
        sum = 0
        for i in vs:
            sum += 1
        return (k, sum)
			
##########################################################################
##########################################################################

from scipy import sparse
def createSparseMatrix(X, label):
	sparseX = sparse.coo_matrix(X)
	list = []
	for i,j,v in zip(sparseX.row, sparseX.col, sparseX.data):
		list.append(((label, i, j), v))
	return list

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ##run WordCount:
    print("\n\n*****************\n Word Count\n*****************\n")
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8, "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
	    (10, "Car engines purred and the tires burned.")]
    print("\nWord Count Basic WITHOUT Combiner:")
    mrObjectNoCombiner = WordCountMR(data, 4, 3)
    mrObjectNoCombiner.runSystem()
    print("\nWord Count Basic WITH Combiner:")
    mrObjectWCombiner = WordCountMR(data, 4, 3, use_combiner=True)
    mrObjectWCombiner.runSystem()
  
    ##################
    #run Matrix Multiply:
    print("\n\n*****************\n Matrix Multiply\n*****************\n")
    #format: 'A|B:A.size:B.size
    test1 = [(('A:1,2:2,1', 0, 0), 2.0), (('A:1,2:2,1', 0, 1), 1.0), (('B:1,2:2,1', 0, 0), 1), (('B:1,2:2,1', 1, 0), 3)   ]
    test2 = createSparseMatrix([[1, 2, 4], [4, 8, 16]], 'A:2,3:3,3') + createSparseMatrix([[1, 1, 1], [2, 2, 2], [4, 4, 4]], 'B:2,3:3,3')
    
    test3 = createSparseMatrix(np.random.randint(-10, 10, (5,20)), 'A:5,20:20,4') + \
	    createSparseMatrix(np.random.randint(-10, 10, (20,4)), 'B:5,20:20,4')

    mrObject = MatrixMultMR(test1, 4, 3)
    mrObject.runSystem()

    mrObject = MatrixMultMR(test3, 16, 10)
    mrObject.runSystem()

    ##################
    #run counts by powers of 10
    print("\n\n*************************\n Count By Powers of 10 \n*************************\n")
    filename = sys.argv[1]
    data = []
    with open(filename, 'r') as infile:
        data = [(int(i.strip()), 1) for i in infile.readlines()]
        
    print("\nExample of input data: ", data[:10])
    mrObject = CountBy10PowersMR(data, 4, 3)
    mrObject.runSystem()
  