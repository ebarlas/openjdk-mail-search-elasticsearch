#!/bin/bash
set -euo pipefail

rm -rf build && mkdir build
cp src/*.py build/

cd build
zip -r ../function.zip .
cd ..

aws lambda update-function-code \
  --function-name openjdk-mail-es-updater \
  --zip-file fileb://function.zip \
  --region us-west-1 \
  --profile personal
