#!/bin/bash

# Gets the project root path
BASEDIR="$( cd "$( dirname "${BASH_SOURCE}" )" >/dev/null 2>&1 && pwd)"
BASEDIR=$(realpath "$BASEDIR")

# Checks that this script is sourced
if [[ $0 != "/bin/bash" && $0 != "bash" && $0 != "-bash" ]]; then
    echo -e "\e[31;1mScript needs to be run with 'source'! source activate.sh\e[0m" >&2
    exit 1
fi

# Deactivate virtual env
conda -V > /dev/null
if [[ $? == 0 ]]; then
    echo "Deactivating virtual env..."
    conda deactivate
fi

export PYTHONPATH="."

# Removes all command aliases
unalias odb-lint odb-test odb-profile odb-help odb-docs odb-mem-profile 2> /dev/null

echo "Done!"
