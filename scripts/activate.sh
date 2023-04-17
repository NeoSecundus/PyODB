#!/bin/bash

# Gets the project root path
BASEDIR="$( cd "$( dirname "${BASH_SOURCE}" )" >/dev/null 2>&1 && pwd)/.."
BASEDIR=$(realpath "$BASEDIR")

# Checks that this script is sourced
if [[ $0 != "/bin/bash" && $0 != "bash" && $0 != "-bash" && $0 != "/usr/bin/bash" ]]; then
    echo -e "\e[31;1mScript needs to be run with 'source'! source activate.sh\e[0m" >&2
    exit 1
fi

# Activate virtual environment
conda -V > /dev/null
if [[ $? == 0 ]]; then
    echo "Activating virtual env..."
    conda activate pyodb
fi

export PYTHONPATH="$BASEDIR/src"

# Creating aliases
alias odb-lint="ruff --config $BASEDIR/pyproject.toml $BASEDIR"
alias odb-test="python -m pytest --cov='./src' --cov-report='html' $BASEDIR/test"
alias odb-profile='python3 -m cProfile -o profiler/results_$(date +%y%m%d-%H%M).prof '
alias odb-mem-profile='memray run -f -o profiler/mem_profile_$(date +%y%m%d-%H%M).bin '
alias odb-docs="pdoc -o docs/module_docs/ -d google --logo $BASEDIR/res/img/Logo.png --favicon $BASEDIR/res/img/Logo.png src"
alias odb-help="echo \"PyODB Repo Commands:
--------------------------
odb-lint - Runs linter and writes logs to lint.log.
odb-test - Runs all tests and generates coverage report.
odb-profile <script> - Profiles the script/program and writes output to profiler/results_<date>.prof
    You can then use snakeviz to visualize the profiling: snakeviz profiler/results_<date>.prof
odb-mem-profile <script> - Profiles the script/program and writes output to profiler/mem_profile_<date>.bin
    You can use multiple builtin visualization tools from memray:
        memray summary <bin_file>
        memray flamegraph <bin_file>
        memray table <bin_file>
        memray tree <bin_file>
        memray stats <bin_file>
        memray transform <bin_file>
odb-docs - Generates an updated module documentation and saves it to docs/module_docs.
odb-help - Show this help.\"
"

echo "Done!"
echo -e "\e[33mEnter odb-help for command listing\e[0m"
