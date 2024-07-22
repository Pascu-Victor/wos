#check that all .hpp files in PWD begin with a #pragma once

for i in $(find . -name "*.hpp"); do
    if [ "$(head -n 1 $i)" != "#pragma once" ]; then
        echo "Error: $i does not begin with #pragma once"
        exit 1
    fi
done
exit 0