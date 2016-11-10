#!/bin/bash
#==========================================================================
#      Filename:  test.sh
#       Created:  2016-11-10 四 13:10
#
#   DESCRIPTION:
#
#        Author:  Yu Yang
#         Email:  plusyuy@NOSPAM.gmail.com
#==========================================================================

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
pdir="$(dirname "$cur_dir")"
ppdir="$(dirname "$pdir")"

export GOPATH=$ppdir
cd $cur_dir

num_loop=10

for ((i=0;i<$num_loop;i++)); do
    go test -run $1
done
