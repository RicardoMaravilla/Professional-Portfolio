###

###

# Bubble Sort
def bubbleSort(array_to_sort):
    swapped_bubble = False
    n = len(array_to_sort)
    # For for all the elements in the array
    for i in range(n-1):
        # 2nd for for the sort method
        for j in range(0, n-i-1):
            # look in the array from 0 to n-i-1
            # Swap if the element found is greater
            # than the next element in the array
            if array_to_sort[j] > array_to_sort[j + 1]:
                swapped_bubble = True
                array_to_sort[j], array_to_sort[j + 1] = array_to_sort[j + 1], array_to_sort[j]

        if not swapped_bubble:
            # if we haven't needed to make a single swap, we
            # can just exit the main loop.
            return

    return array_to_sort

# Selection Sort
def selectionSort(array):
    size = len(array)
    for step in range(size):
        min_idx = step

        for i in range(step + 1, size):
            # to sort in descending order, change > to < in this line
            # select the minimum element in each loop
            if array[i] < array[min_idx]:
                min_idx = i
        # put min at the correct position
        (array[step], array[min_idx]) = (array[min_idx], array[step])

    return array

# Insert Sort
def insertionSort(array):
    # Traverse through 1 to len(arr)
    for i in range(1, len(array)):

        key = array[i]

        # Move elements of arr[0..i-1], that are
        # greater than key, to one position ahead
        # of their current position
        j = i-1
        while j >= 0 and key < array[j] :
                array[j + 1] = array[j]
                j -= 1
        array[j + 1] = key

    return array

# Merge Sort
def merge(array, l, m, r):
    n1 = m - l + 1
    n2 = r - m

    # create temp arrays
    L = [0] * (n1)
    R = [0] * (n2)

    # Copy data to temp arrays L[] and R[]
    for i in range(0, n1):
        L[i] = array[l + i]

    for j in range(0, n2):
        R[j] = array[m + 1 + j]

    # Merge the temp arrays back into arr[l..r]
    i = 0     # Initial index of first subarray
    j = 0     # Initial index of second subarray
    k = l     # Initial index of merged subarray

    while i < n1 and j < n2:
        if L[i] <= R[j]:
            array[k] = L[i]
            i += 1
        else:
            array[k] = R[j]
            j += 1
        k += 1

    # Copy the remaining elements of L[], if there
    # are any
    while i < n1:
        array[k] = L[i]
        i += 1
        k += 1

    # Copy the remaining elements of R[], if there
    # are any
    while j < n2:
        array[k] = R[j]
        j += 1
        k += 1

def mergeSort(array, l, r):
    if l < r:

        # Same as (l+r)//2, but avoids overflow for
        # large l and h
        m = l+(r-l)//2

        # Sort first and second halves
        mergeSort(array, l, m)
        mergeSort(array, m+1, r)
        merge(array, l, m, r)

    return array
