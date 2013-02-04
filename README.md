file_name_set_manager
=====================

This program will maintain redis sets of of file names by watching a directory 
with inotify.

At SpiderOak, we use this to manage groups of files that are passed around 
nodes in our internal cluster.  We were doing a listdir of our 'transport'
directory and pickout out files with a common transaction identifier. This 
becomes unacceptably slow for large numbers of files. So we developed this
program to track the files related to a single transaction without the 
sequential search.

Contact: Doug Fort dougfort@spideroak.com
    