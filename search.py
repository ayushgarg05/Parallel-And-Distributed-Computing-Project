from pyspark.sql import SparkSession
import time
import random

start_time = time.time()  # Taking Starting Time 
spark = SparkSession.builder.appName("search").getOrCreate() # Creating Spark Session in order to Start Apache Spark
random_items = [random.randint(-100, 100) for c in range(500)] # Generating Random Numbers

print(random_items)      #Printing Array List

def linear_search(list_of_elements,x):
    found = False
    for i in range(len(list_of_elements)):
        if(list_of_elements[i] == x):
            found = True
            print("%d found at %dth position"%(x,i))
            break
    if(found == False):
        print("%d is not in list"%x)


def binarySearch (arr, l, r, x):
    if r >= l:
        mid = int(l + (r - l)/2)
        if arr[mid] == x: #Element present in Middle
            return mid
        elif arr[mid] > x:  #Element present in Left SubArray
            return binarySearch(arr, l, mid-1, x)
        else:
            return binarySearch(arr, mid+1, r, x)  #Element present in Right SubArray
    else:
        return -1   # Element not present

#x = int(input("\n\nEnter number to search: "))
x=8
linear_search(random_items,x) # Calling the Linear Search Function

#result = binarySearch(random_items, 0, len(random_items)-1, x) # Calling The Binary Search Function

#if result != -1:
#    print ("\n\n%d is present at index %d" % x,result)
#else:
#    print ("%d is not present in array")

print("\n\n--- Time Taken In Linear Search:-  %s seconds ---\n" % (time.time() - start_time))
spark.stop()
