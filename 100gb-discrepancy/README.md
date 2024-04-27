NOTE: Current working solution is only in python

## Problem Statement

Imagine a CSV file with 100GB of coordinate data (x, y) columns. We take this file and pick a random 20% of the rows, change the y value to something different, and then store all the rows in a new file with random row order. Give a basic approach to how you will find these mismatched coordinate pairs and write them to a file. In short, find all the coordinates in the first file which had their y values changed.


### Solution

First simple approach is to use External Sort (For sorting files which don't fit in memory) on both files, and read row by row from both file, and finding mismatches.

More efficient approach is to use hash tables, where we store hashes of all rows in original file and check for existence of each row in modified file. As the file is large, we can use Disk-based hash tables (https://stackoverflow.com/questions/495161/fast-disk-based-hashtables)