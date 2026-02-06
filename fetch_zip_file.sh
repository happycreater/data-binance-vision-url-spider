#!usr/bin/env bash
set -euo pipefail

# 输入为zip文件路径
ZIP_PATH=$1
# 解压并覆盖当前目录下的文件
unzip -o $ZIP_PATH

# 逐个处理解压后的文件
for file in *.txt; do
    python fetch_zip_file.py --url-file $file
done