#check that all .hpp files in PWD begin with a #pragma once
set -e
cd $1
for i in $(find . -name "*.hpp"); do
    if [ "$(head -n 1 $i)" != "#pragma once" ]; then
        echo "Error: $i does not begin with #pragma once"
        echo "Actual:"
        head -n 1 $i
        exit 1
    fi
done
cd - > /dev/null
exit 0
