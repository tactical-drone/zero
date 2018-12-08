#!/bin/bash
TARGET_DIR="bin/Debug/netcoreapp2.1"
ENTANGLED_DIR="../entangled"

#cp ../../entangled/bazel-bin/common/trinary/libptrits.so $TARGET_DIR/

#cp ../../entangled/bazel-bin/common/trinary/libtrit_byte.so* $TARGET_DIR/

#cp ../../entangled/bazel-bin/common/trinary/libtrit_tryte.so* $TARGET_DIR/

#cp ../../entangled/bazel-bin/common/trinary/libflex_trit*.so* $TARGET_DIR/
#cp $TARGET_DIR/libflex_trit_5.so $TARGET_DIR/libflex_trit.so

#cp ../../entangled/bazel-bin/common/trinary/libtrit_long.so* $TARGET_DIR/

#cp ../../entangled/bazel-bin/common/curl-p/libtrit.so* $TARGET_DIR/

#cp ../../entangled/bazel-bin/common/curl-p/libdigest.so* $TARGET_DIR/

#cp ../../entangled/bazel-bin/common/model/libtransaction.so* $TARGET_DIR/

cp $ENTANGLED_DIR/bazel-bin/common/libinterop.so zero.sync/$TARGET_DIR/
cp $ENTANGLED_DIR/bazel-bin/common/libinterop.so zero.api/$TARGET_DIR/

