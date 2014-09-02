import unittest
import numpy as np
import csv
import sys
sys.path.extend(['./../../','./../GeoCodes'])
sys.dont_write_bytecode = True

from csv_handler import readcsv, appendcsv

class TestCsvHandler(unittest.TestCase):
    def setUp(self):
        self.infile = "./in/Completeset.csv"        
    def test_readcsv(self): # test csv reader
        fileread = readcsv(self.infile) # file read
        compare = csv.reader(open(self.infile)) # used for comparison
        nrows = sum(1 for row in compare) # total number of rows
        self.assertEqual(len(fileread),nrows) # shd have same lengths
        # Elements read shd be unique. Identified by address. Exception -9999.
        addresses = [case[0] for case in fileread] # all addresses
        unknowns = addresses.count('-9999') # address not known
        empty = addresses.count("")
        print "There are total ", len(addresses), "cases."
        print unknowns, "cases are with address unknown."
        print empty, "cases leave address blank."
        print len(addresses) - unknowns - empty - (len(set(addresses)) - 1), \
                "cases are not unique in address."









if __name__ == '__main__':
    unittest.main()
