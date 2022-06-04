##########################################################################
## streamingCSE545sp22_lastname_id.py  v1
## 
## Template code for assignment 1 part 1. 
## Do not edit anywhere except blocks where a #[TODO]# appears
##
## Student Name: Nicole Niemiec
## Student ID: #112039349


import sys
from pprint import pprint
import numpy as np
import random
from collections import deque
from sys import getsizeof
import resource
import math

from sympy import N

##########################################################################
##########################################################################
# Methods: implement the methods of the assignment below.  
#
# Each method gets 1 100 element array for holding ints of floats. 
# This array is called memory1a, memory1b, or memory1c
# You may not store anything else outside the scope of the method.
# "current memory size" printed by main should not exceed 8,000.

# pick number of hashes ? has to be <= 100 i think
# hooooooow do i do that
# what is memory

def get_mode(index):
    values = {
    0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8,
    8: 9, 9: 10, 10: 11, 11: 20, 12: 30, 13: 40, 14: 50,
    15: 60, 16: 70, 17: 80, 18: 90, 19: 100, 20: 110, 21: 120, 
    22: 130, 23: 140, 24: 150, 25: 160, 26: 170, 27: 180, 28: 190,
    29: 200, 30: 300, 31: 400, 32: 500, 33: 600, 34: 700, 35: 800,
    36: 900, 37: 1000, 38: 1100, 39: 1200, 40: 1300, 41: 1400,
    42: 1500, 43: 1600, 44: 1700, 45: 1800, 46: 1900, 47: 2000,
    48: 3000, 49: 4000, 50: 5000, 51: 6000, 52: 7000, 53: 8000,
    54: 9000, 55: 10000, 56: 20000, 57: 30000, 58: 40000, 59: 50000,
    60: 60000, 61: 70000, 62: 80000, 63: 90000, 64: 100000,
    65: 1000000
    }
    return values[index]

def find_index(element):
    values = {
    0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8,
    8: 9, 9: 10, 10: 11, 11: 20, 12: 30, 13: 40, 14: 50,
    15: 60, 16: 70, 17: 80, 18: 90, 19: 100, 20: 110, 21: 120, 
    22: 130, 23: 140, 24: 150, 25: 160, 26: 170, 27: 180, 28: 190,
    29: 200, 30: 300, 31: 400, 32: 500, 33: 600, 34: 700, 35: 800,
    36: 900, 37: 1000, 38: 1100, 39: 1200, 40: 1300, 41: 1400,
    42: 1500, 43: 1600, 44: 1700, 45: 1800, 46: 1900, 47: 2000,
    48: 3000, 49: 4000, 50: 5000, 51: 6000, 52: 7000, 53: 8000,
    54: 9000, 55: 10000, 56: 20000, 57: 30000, 58: 40000, 59: 50000,
    60: 60000, 61: 70000, 62: 80000, 63: 90000, 64: 100000,
    65: 1000000
    }
    if element <= 11:
        return element - 1
    else:
        for i in values.keys():
            if element < values[i]:
                return i - 1


def create_hash_functions():
    list = []
    for i in range(0, 100):
        a = random.randint(1, 99)
        b = random.randint(1, 99)
        # c = 10000000
        c = 943661897
        list.append((a, b, c))
    return list

def find_trailing_zeroes(i):
    binary = bin(i)
    length = len(binary) - len(binary.rstrip('0'))
    return length

def do_hashing(a, b, c, element):
    hash = ((a*element) + b) % c 
    return hash

def find_mean(list):
    return (sum(list) / len(list))

MEMORY_SIZE = 100 #do not edit
# static objects
hashes = create_hash_functions()
memory1a =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit
# need to create a bunch of hash functions hmm
# can hard code them
# maybe use as close to 100 as possible ?
# welp i tried
def task1ADistinctValues(element, returnResult = True):
    count = 0
    for h in hashes:
        (a, b, c) = h
        hash = do_hashing(a, b, c, element)
        num_zeroes = find_trailing_zeroes(hash)
        if memory1a[count] == None:
            memory1a[count] = num_zeroes
        elif num_zeroes > memory1a[count]:
            memory1a[count] = num_zeroes
        count = count + 1
    #process the element you may only use memory1a, storing at most 100 
    if returnResult: #when the stream is requesting the current result
        result = 0
        # print(memory1a)

        list = []
        counter = 0
        for group in range(0,100,math.floor(math.log2(100))):
            group_hashes = []
            zeroes = 0
            for i in range(0, math.floor(math.log2(100))):
                group_hashes.append(memory1a[counter+i])
                # if memory1a[counter+i] == 0:
                #     if zeroes < 2:
                #         group_hashes.append(memory1a[counter+i])
                #         zeroes = zeroes + 1
                #     else:
                #         zeroes = zeroes + 1
                # else:
                #     group_hashes.append(memory1a[counter+i])
            counter = group
            list.append(find_mean(group_hashes))

        result = np.median(np.array(list))
        # print(result)
        result = 2**result
        # print(result)
        # print(result)
        #any additional processing to return the result at this point
        return result
    else: #no need to return a result
        pass


memory1b =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit
# follows a pareto distribution
def task1BMedian(element, returnResult = True):
    # this if will only run once (when the first element is seen)
    if memory1b[0] == None and memory1b[1] == None and memory1b[2] == None and memory1b[3] == None and memory1b[4] == None:
        memory1b[0] = 0
        memory1b[1] = 0
        memory1b[2] = 0
        memory1b[3] = 0
        memory1b[4] = 0

    memory1b[0] = memory1b[0] + 1 # keeping track of "n" elements so far
    natural_log = math.log(element) # ln(element)
    memory1b[1] = memory1b[1] + natural_log # this will be the bottom running summation, sum of ln(x_i) from i to n
    alpha = memory1b[0] / memory1b[1] # alpha = n / sum of ln(x_i) from i to n
    p = 1 - math.pow((1/element), alpha) # P(X < x) = 1 - (1/x)^alpha

    if p < .5999 and p > .49999 and memory1b[4] == 0: # worst case median
        memory1b[2] = element
    if p < .559 and p > .539 and memory1b[4] == 0: # second worst case median
        memory1b[3] = element
    if p < .519 and p > .499: # closest median
        memory1b[4] = element
    # if we have P(X < x) or closest to .5 as possible, then we have the median

    if returnResult: #when the stream is requesting the current result
        if memory1b[4] != 0: # best case median
            return memory1b[4]
        elif memory1b[3] != 0: # next best median
            return memory1b[3]
        else: # worst median
            return memory1b[2]
    else: #no need to return a result
        pass
    

memory1c =  deque([None] * MEMORY_SIZE, maxlen=MEMORY_SIZE) #do not edit
# alrighty what are we gonna do here
# know it's pareto type 1
# or kiiiiiinda pareto type 1
# but we'll ignore the kiiiiiiinda
# this is about to be suuuuper janky
def task1CMostFreqValue(element, returnResult = True):
    #process the element
    # gonna say 100000 is the biggest element
    # x[0 - 9] -> 1-10 in elements, see find_index()
    index = find_index(element)
    # index = 1
    if memory1c[index] == None:
        memory1c[index] = 1
    else:
        memory1c[index] = memory1c[index] + 1 
    
    if returnResult: #when the stream is requesting the current result
        # mode is most, so we find the max
        max = 0
        for i in range(0, 71):
            if memory1c[i] != None: 
                if memory1c[i] > max:
                    max = get_mode(i)
        
        result = max
        return result
    else: #no need to return a result
        pass


##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls your methods
# Printouts of the results returned will be done every so often
# DO NOT EDIT BELOW

def getMemorySize(l): #returns sum of all element sizes
    return sum([getsizeof(e) for e in l])+getsizeof(l)

if __name__ == "__main__": #[Uncomment peices to test]
    
    print("\n\nTESTING YOUR CODE\n")
    
    ###################
    ## The main stream loop: 
    print("\n\n*************************\n Beginning stream input \n*************************\n")
    filename = sys.argv[1]#the data file to read into a stream
    printLines = frozenset([10**i for i in range(1, 20)]) #stores lines to print
    peakMem = 0 #tracks peak memory usage
    
    with open(filename, 'r') as infile:
        i = 0#keeps track of lines read
        for line in infile:
        
            #remove \n and convert to int
            element = int(line.strip())
            i += 1
            
            #call tasks         
            if i in printLines: #print status at this point: 
                result1a = task1ADistinctValues(element, returnResult=True)
                result1b = task1BMedian(element, returnResult=True)
                result1c = task1CMostFreqValue(element, returnResult=True)
                
                print(" Result at stream element # %d:" % i)
                print("   1A:     Distinct values: %d" % int(result1a))
                print("   1B:              Median: %.2f" % float(result1b))
                print("   1C: Most frequent value: %d" % int(result1c))
                print(" [current memory sizes: A: %d, B: %d, C: %d]\n" % \
                    (getMemorySize(memory1a), getMemorySize(memory1b), getMemorySize(memory1c)))
                
            else: #just pass for stream processing
                result1a = task1ADistinctValues(element, False)
                result1b = task1BMedian(element, False)
                result1c = task1CMostFreqValue(element, False)
                
            memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            if memUsage > peakMem: peakMem = memUsage
        
    print("\n*******************************\n       Stream Terminated \n*******************************")
    print("(peak memory usage was: ", peakMem, ")")