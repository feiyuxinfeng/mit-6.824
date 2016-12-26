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

tmpfile=$(mktemp /tmp/abc-script.XXXXXX)

test_name=""
num_loop=10

if [ "$#" -eq 1 ]; then
    re='^[0-9]+$'
    if ! [[ $1 =~ $re ]] ; then
        # echo "error: Not a number" >&2; exit 1
        test_name=$1
    else
        num_loop=$1
    fi
elif [ "$#" -eq 2 ]; then
    test_name=$1
    num_loop=$2
fi

echo "test name: $test_name"
echo "loop $num_loop times"

for ((i=0;i<$num_loop;i++)); do
    echo "$i +++++"
    if [ -z "$test_name" ]; then
        go test | tee -a $tmpfile
    else
        go test -run $test_name | tee -a $tmpfile
    fi
done

pass=`grep ^PASS$ $tmpfile | wc -l`
failed=`grep ^FAIL$ $tmpfile | wc -l`
echo
echo "========= result ====================="
echo
echo "sucess: $pass"
echo "fail: $failed"
echo
echo "======== end ======================="
rm "$tmpfile"

if [ "$failed" != "0" ]; then
    echo
    echo "tests failed."
    exit 1
else
    echo "tests success."
    exit 0
fi
