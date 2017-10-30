from pyspark.sql import SparkSession
import time
import random

start_time = time.time()  # Taking Starting Time 
spark = SparkSession.builder.appName("sort").getOrCreate() # Creating Spark Session in order to Start Apache Spark
random_items = [random.randint(-100, 100) for c in range(100)] # Generating Random Numbers
 
#BUBBLE SORT
def bubble_sort(items):
    """ Implementation of bubble sort """
    for i in range(len(items)):
        for j in range(len(items)-1-i):
            if items[j] > items[j+1]:
                items[j], items[j+1] = items[j+1], items[j]     #Swapping Operation Takes Place
 
#INSERTION SORT
def insertion_sort(items):
    """ Implementation of insertion sort """
    for i in range(1, len(items)):
        j = i
        while j > 0 and items[j] < items[j-1]:
            items[j], items[j-1] = items[j-1], items[j]
            j -= 1

#MERGE SORT
def merge_sort(items):
    """ Implementation of mergesort """
    if len(items) > 1:
        mid = int(len(items) / 2)        # Determine the midpoint and split
        left = items[0:mid]
        right = items[mid:]

        merge_sort(left)            # Sort left list in-place
        merge_sort(right)           # Sort right list in-place
        l, r = 0, 0
        for i in range(len(items)):     # Merging the left and right list
            lval = left[l] if l < len(left) else None
            rval = right[r] if r < len(right) else None
            if (lval and rval and lval < rval) or rval is None:
                items[i] = lval
                l += 1
            elif (lval and rval and lval >= rval) or lval is None:
                items[i] = rval
                r += 1
            else:
                raise Exception('Could not merge, sub arrays sizes do not match the main array')
 
#QUICK SORT
def quick_sort(items):
    """ Implementation of quick sort """
    
    if len(items) > 1:
        pivot_index = int(len(items) / 2)
        smaller_items = []
        larger_items = []
        for i, val in enumerate(items):
            if i != pivot_index:
                if val < items[pivot_index]:
                    smaller_items.append(val)
                else:
                    larger_items.append(val)
        quick_sort(smaller_items)
        quick_sort(larger_items)
        items[:] = smaller_items + [items[pivot_index]] + larger_items

#HEAP SORT
import heapq

def heap_sort(items):
    """ Implementation of heap sort """
    heapq.heapify(items)
    items[:] = [heapq.heappop(items) for i in range(len(items))]


print ('Before Sorting:')
print(random_items)      #Printing Array List Before Sorting
print("\n\n")

quick_sort(random_items) #Calling The Sorting Function

print ('\n\nAfter Sorting: ')
print(random_items)      #Printing Array List After Sorting

print("\n\n\n---Time Taken For Quick Sort:-  %s seconds ---" % (time.time() - start_time))
spark.stop()
