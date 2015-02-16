#!/usr/bin/python

import os
import random
import re
import sys

if len(sys.argv) != 2:
    print "Usage: python gen-twitter.py <numrecords>"
    sys.exit(1)

def loadfile(filename):
    handle = open(filename, 'r')

    retval = []
    for line in handle.readlines():
        retval.append(line.strip())

    return retval

def randomSentence(words, numwords):

    tojoin = []
    for i in range(0, numwords):
        tojoin.append(words[random.randint(0, len(words)-1)])

    return " ".join(tojoin)


if __name__ == "__main__":

    names = []
    words = []

    if os.path.exists("/usr/share/dict/propernames"):
        names = loadfile("/usr/share/dict/propernames")
    else:
        names = loadfile("/usr/share/dict/words")

    words = loadfile("/usr/share/dict/words")

    fmrinput = open("mapreduce-input.txt", 'w')
    ftest = open("users-for-testing.txt", 'w')
    ftrain = open("users-for-training.txt", 'w')

    allusers = set()

    for i in range(0, int(sys.argv[1])):
        name = re.sub(" ", "", randomSentence(names, 2))
        sentence = randomSentence(words, random.randint(10,20))
        allusers.add(name)
        fmrinput.write("%s\t%s\n" % (name, sentence))

    for user in allusers:
        ftest.write("%s\n" % (user))

        if random.random() < .01:
            ftrain.write("%s\n" % (user))

    fmrinput.close()
    ftest.close()
    ftrain.close()

    sys.exit(0)
